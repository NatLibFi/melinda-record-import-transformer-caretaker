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
import {Utils} from '@natlibfi/melinda-commons';
import {recordActions, linkDataActions} from '@natlibfi/melinda-record-link-migration-commons';

class TransformEmitter extends EventEmitter {}

export default function (stream, {validate = true, fix = true}) {
	MarcRecord.setValidationOptions({subfieldValues: false});
	const {filterExistingFields, addOrReplaceDataFields, replaceValueInField} = recordActions();
	const {convertLinkDataToDataFields} = linkDataActions();
	const {createLogger} = Utils;
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
				console.log(`: Handled ${promises.length} recordEvents`);
				await Promise.all(promises);
				Emitter.emit('end', promises.length);
			});
		} catch (err) {
			Emitter.emit('error', err);
		}
	}

	async function convertRecord(data) {
		logger.log('debug', 'Updating record START *******************************');

		// InputData : {record, hostRecord, changes} OR {record, linkData, changes}

		// Data.record = record to be updated
		const record = new MarcRecord(data.record);
		logger.log('debug', `Record to be updated: ${JSON.stringify(record)}`);

		// Data.changes = update changes
		const changes = data.changes || [];
		logger.log('debug', `${changes.length} update changes: ${JSON.stringify(changes)}`);

		// Data.hostRecord = source record
		const hostRecord = data.hostRecord === undefined ? null : new MarcRecord(data.hostRecord);
		logger.log('debug', `Host record: ${JSON.stringify(hostRecord)}`);

		// Data.linkData = Data to be used in changes
		const linkData = data.linkData === undefined ? null : data.linkData;
		logger.log('debug', `Linked data: ${JSON.stringify(linkData)}`);
		logger.log('debug', '*******************************');

		const resultRecord = await changesPump({changes, hostRecord, linkData, record});

		logger.log('debug', 'Updating record DONE *******************************');
		logger.log('debug', `Updated record: ${JSON.stringify(resultRecord)}`);

		const creationDate = moment().format('YYMMDD');
		let fakeRecord = new MarcRecord({
			leader: '00000ngm a22005774i 4500',
			fields: [
				{
					tag: '008',
					value: `${creationDate}    fi ||| g^    |    v|mul|c`
				},
				{
					tag: '024',
					subfields: [{code: 'a', value: '000000'}]
				},
				{
					tag: '245',
					subfields: [{code: 'a', value: 'foobar'}]
				}
			]
		});

		if (data === false) {
			fakeRecord.appendField({tag: 'FOO', value: 'bar'});
		}

		if (validate === true || fix === true) {
			// Validation works only if inputData is type boolean: true or false.
			return validator(fakeRecord, validate, fix);
		}

		// No validation or fix = all succes!
		return {failed: false, record: {...fakeRecord}};
	}

	async function changesPump({changes, hostRecord, linkData, record}) {
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
			const updatedRecord = await addOrReplaceDataFields(record, uniqueLinkDataFields, change);
			logger.log('debug', `Updated record after add ${JSON.stringify(updatedRecord)}`);
			return changesPump({changes: rest, hostRecord, linkData, record: updatedRecord});
		}

		if (change.from !== undefined && change.to !== undefined) {
			logger.log('debug', 'Replace change!');
			/* Example
			from: {tag: '001', value: 'value'}, // From host record
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

			const updatedRecord = await replaceValueInField(hostRecord, record, change);
			logger.log('debug', `Updated record after change ${JSON.stringify(updatedRecord)}`);
			return changesPump({changes: rest, hostRecord, linkData, record});
		}

		return changesPump({changes: rest, hostRecord, linkData, record});
	}
}
