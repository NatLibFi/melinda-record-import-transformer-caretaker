/**
*
* @licstart  The following is the entire license notice for the JavaScript code in this file.
*
* Record link migration transformer for the Melinda record batch import system
*
* Copyright (C) 2018 University Of Helsinki (The National Library Of Finland)
*
* This file is part of melinda-record-import-transformer-record-link-migration
*
* melinda-record-import-transformer-record-link-migration program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* melinda-record-import-transformer-record-link-migration is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
* @licend  The above is the entire license notice
* for the JavaScript code in this file.
*
*/

import {chain} from 'stream-chain';
import {parser} from 'stream-json';
import {streamArray} from 'stream-json/streamers/StreamArray';
import {MarcRecord} from '@natlibfi/marc-record';
import validator from './validate';
import moment from 'moment';
import {EventEmitter} from 'events';
import {recordActions, linkDataActions} from '@natlibfi/melinda-record-link-migration-commons';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {logError} from '@natlibfi/melinda-record-link-migration-commons/dist/utils';

class TransformEmitter extends EventEmitter {}

export default function (stream, {validate = true, fix = true}) {
	const {filterExistingFields, addOrReplaceDataFields, replaceValueInField, removeSubfields} = recordActions();
	const {convertLinkDataToDataFields} = linkDataActions();
	const logger = createLogger();
	const Emitter = new TransformEmitter();

	readStream(stream);
	return Emitter;

	async function readStream(stream) {
		let promises = [];

		try {
			const pipeline = chain([
				stream,
				parser(),
				streamArray()
			]);

			pipeline.on('data', async data => {
				promises.push(transform(data.value));
				async function transform(value) {
					const result = await convertRecord(value);
					Emitter.emit('record', result);
				}
			});
			pipeline.on('end', async () => {
				logger.log('info', `Handled ${promises.length} record events from data`);
				await Promise.all(promises);
				Emitter.emit('end', promises.length);
			});
		} catch (err) {
			logError(err);
			Emitter.emit('error', err);
		}
	}

	async function convertRecord(data) {
		logger.log('debug', 'Updating record START *******************************');

		// InputData : {record, sourceRecord, changes} OR {record, linkData, changes}

		// Data.record = record to be updated
		const record = new MarcRecord(data.record, {subfieldValues: false});
		logger.log('silly', `Record to be updated: ${JSON.stringify(record)}`);

		// Data.changes = update changes
		const changes = data.changes || [];
		logger.log('silly', `${changes.length} update changes: ${JSON.stringify(changes)}`);

		// Data.sourceRecord = source record
		const sourceRecord = data.sourceRecord === undefined ? null : new MarcRecord(data.sourceRecord, {subfieldValues: false});
		logger.log('silly', `Source record: ${JSON.stringify(sourceRecord)}`);

		// Data.linkData = Data to be used in changes
		const linkData = data.linkData === undefined ? null : data.linkData;
		logger.log('silly', `Linked data: ${JSON.stringify(linkData)}`);
		logger.log('debug', 'Link data handled *******************************');
		const originalRecordClone = MarcRecord.clone(record, {subfieldValues: false});

		const resultRecord = await changesPump({changes, sourceRecord, linkData, record});

		logger.log('debug', 'Updating record DONE *******************************');
		const updateNeeded = !originalRecordClone.equalsTo(resultRecord);
		logger.log('info', `Needs to be updated: ${updateNeeded}`);

		if (updateNeeded) {
			logger.log('debug', `Record to be updated: ${JSON.stringify(originalRecordClone.toObject())}`);
			logger.log('debug', `Source record: ${JSON.stringify(sourceRecord.toObject())}`);
			logger.log('debug', `Linked data: ${JSON.stringify(linkData)}`);
			logger.log('debug', `Updated record: ${JSON.stringify(resultRecord.toObject())}`);

			logger.log('debug', '*******************************');
			return {failed: false, record: {...resultRecord.toObject()}};
		}

		logger.log('debug', '*******************************');
		return {record: resultRecord.toObject(), failed: true, messages: ['No update needed!']};
	}

	async function changesPump({changes, sourceRecord, linkData, record}) {
		const [change, ...rest] = changes;
		if (change === undefined) {
			logger.log('debug', 'Changes done!');
			return record;
		}

		logger.log('debug', 'Pumping change!');
		logger.log('debug', `Change: ${JSON.stringify(change)}`);
		logger.log('debug', `Next: ${JSON.stringify(rest)}`);
		logger.log('debug', '*******************************');

		if (change.add !== undefined) {
			logger.log('debug', 'Add change!');

			/* Example
				add: {tag: '650', ind1: ' ', ind2: '7', subfields: [{code: 'a', value: '%s'}, {code: '2', value: 'yso/%s'}, {code: '0', value: 'http://www.yso.fi/onto/yso/%s'}]}, // To result record
				order: ['a', '2', '0'],
				duplicateFilterCodes: ['2', '0']
			*/
			const linkDataFields = await convertLinkDataToDataFields(linkData, change);
			logger.log('debug', JSON.stringify(linkDataFields));
			const uniqueLinkDataFields = await filterExistingFields(linkDataFields, record);
			logger.log('debug', JSON.stringify(uniqueLinkDataFields));
			await addOrReplaceDataFields(record, uniqueLinkDataFields, change);
			logger.log('debug', `Updated record after add ${JSON.stringify(record)}`);
			return changesPump({changes: rest, sourceRecord, linkData, record});
		}

		if (change.from !== undefined && change.to !== undefined) {
			logger.log('debug', 'Replace change!');
			/* Example
			from: {tag: '001', value: 'value'}, // From source record
			to: {
				tag: '100',
				value: {code: '0'},
				format: `(FIN11)%s`,
				where: {
					collect: ['a', 'b', 'c', 'd', 'q'],
					from: {tag: '100', value: 'collect'},
					to: {tag: '100', value: 'collect'}
				  }
			}, // To result record
			order: ['a', 'c', 'q', 'd', 'e', '0'] // Subfield sort order after modify
			*/

			await replaceValueInField(sourceRecord, record, change);
			// Logger.log('debug', `Updated record after change ${JSON.stringify(updatedRecord)}`);
			return changesPump({changes: rest, sourceRecord, linkData, record});
		}

		if (change.removeSubfields !== undefined) {
			logger.log('debug', 'Remove change!');
			// RemoveSubfields: {tag: '100', code: '0', value: 'digitsOnly'}
			removeSubfields(record, change.removeSubfields);
			return changesPump({changes: rest, sourceRecord, linkData, record});
		}

		return changesPump({changes: rest, sourceRecord, linkData, record});
	}
}
