import fs from 'fs';
import path from 'path';
import AdmZip from "adm-zip";
import lang from 'detect-file-encoding-and-language';
import { promisify } from 'util';
import got from 'got';
import stream from 'stream'
import neatCsv from 'neat-csv';
import Analytics from "@rudderstack/rudder-sdk-node";

async function downloadReport(url, ofile){
	const pipeline = promisify(stream.pipeline);
	await pipeline(
		got.stream(url),
		fs.createWriteStream(ofile)
	);
}

function unzipReport(dlFile){
	const reportZip = new AdmZip(dlFile);
	reportZip.extractAllTo('/tmp');
};

function findCSVs(){
	const files = fs.readdirSync('/tmp').filter(i => i.endsWith('.csv') && !i.endsWith('.utf8.csv'))
	console.log(files);
	return files;
};


export default defineComponent({
  async run({ steps, $ }) {
    const reportURL = steps.trigger.event.body.text.match(/https:\/\/[A-Za-z]+\.getstat.com\/reports\/.*/)[0];
    const dlFile = path.resolve('/tmp', 'report.zip');

		await downloadReport(reportURL, dlFile);
		unzipReport(dlFile);
		const files = findCSVs();
		for (const file of files){
			const ifile = path.resolve('/tmp', file);
			const encoding = await lang(ifile);

			const ifileData = fs.readFileSync(ifile, {encoding: encoding.encoding})
			const csv = await neatCsv(ifileData, {
				separator: '\t', 
				mapHeaders: ({header, index}) => {
					let baseTransform = header.toLocaleLowerCase().replace(/\s+/g, '_');
					if (baseTransform.startsWith('_')){
						baseTransform = baseTransform.slice(1)
					}
					return baseTransform;
				}
			});

			// const csv is now an array of objects with normalized property names
			
			const WRITE_KEY = ''; // change me
			const DATA_PLANE_URL = ''// change me
			const client = new Analytics(WRITE_KEY, `${DATA_PLANE_URL}/v1/batch`)

			csv.forEach(row => {
				client.track({
					userId: "pipedream_importer",
					event: "moz_stat_report",
					properties: row
				});
			});

			client.flush()
		}
		
		


    return steps.trigger.event
  },
})