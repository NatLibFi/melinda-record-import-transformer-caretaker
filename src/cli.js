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

import transform from './transform';
import {Transformer} from '@natlibfi/melinda-record-import-commons';

const {runCLI} = Transformer;

run();

async function run() {
	const transformerSettings = {
		name: 'melinda-record-import-transformer-dummy',
		yargsOptions: [
			{option: 'v', conf: {alias: 'validate', default: false, type: 'boolean', describe: 'Validate records'}},
			{option: 'f', conf: {alias: 'fix', default: false, type: 'boolean', describe: 'Validate & fix records'}}
		],
		callback: transform
	};
	runCLI(transformerSettings);
}
