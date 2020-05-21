// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================

/**
 * Creates the PO Comparison chart using ChartJS and stores it in Cloud Storage.
 *
 * NOTE: When there is more than one chart we will split this up into a module for
 * agnostic ChartJS functions and modules for each chart.  For now it is all in
 * the same place.
 */

const md5 = require('md5');
const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');
const storage = new Storage();

const COLORS = [
  '#1b2126',
  '#6d1e7d',
  '#ff2454'
];

const STORAGE_BUCKET_IMAGES = `${process.env.GCP_PROJECT}-report-images`;

/**
 * The entry point for the Cloud Function.  The end result is a new chart image
 * in Cloud Storage and a message sent to the generate email function.
 * @param {Object} event - Event object passed from Cloud Pub/Sub
 * @param {Object} event.data - Message data.  Expected to be a JSON representation
 * of all the data for this recipient. Only some of it is used here but all of
 * it is passed on to email generation.  Properties needed to generate a chart
 * are email_address, positive_discharges, district_average, state_average.
 * @param {Object} context - Metadata from Pub/Sub.  Not used.
 */
exports.report_create_po_comparison_chart = async (event, context) => {
    const pubsubMessage = event.data;
    let jsonStr = Buffer.from(pubsubMessage, 'base64').toString();
    /*let jsonObj;
    try {
        jsonObj = JSON.parse(jsonStr);
    } catch(err) {
        console.error(`Error parsing JSON passed in through Pub/Sub: ${jsonStr}`);
        throw err;
    }
    const configuration = createConfiguration(jsonObj);
    const image = await createChartImage(configuration);

    /* TODO: The assumption here is that we are retrieving images from a URL.  This
     * article (https://medium.com/@emailmonks/top-expert-tips-on-how-to-embed-images-in-html-emails-4f13b3784472)
     * says this is the best practice but we could also base64 encode the images.
     * This decision will be made with our email expert contractor.  If we
     * choose to store images on a server they will need to be in a public CDN
     * folder and the filename will need to be anonymized.
     */
    /*if (jsonObj.email_address == undefined) {
        let msg = `Unable to generate a PO Comparison chart because `
            + `email_address property does not exist. full data = ${JSON.stringify(jsonObj)}`;
        throw new TypeError(msg);
    }
    const imageName = md5(`po comparison chart ${jsonObj.email_address}`) + ".png";
    try {
        writeImageToBucket(image, STORAGE_BUCKET_IMAGES, imageName);
    } catch(err) {
        console.error(`Unable to write image '${imageName}' to bucket `
            + `'${STORAGE_BUCKET_IMAGES}'. batch id = ${jsonObj.batch_id}, email address `
            + `= ${jsonObj.email_address}`);
        throw err;
    }
    console.log(`Created image for ${jsonObj.email_address} named ${imageName}`);

    jsonObj.image_name = imageName;
    jsonStr = JSON.stringify(jsonObj); */
    try {
        await sendMessageToGeneration(jsonStr);
    } catch(err) {
        console.error(`Error while sending Pub/Sub message to start generation. `
            + `Recipient data = ${jsonStr}`);
        throw err;
    }
}

/**
 * Generate the configuration object for chartjs
 * @param {Object} data - The user data needed to create the chart in the raw
 * format passed via Pub/Sub
 * @returns {Object} An object in the format required by ChartJS for this chart
 */
function createConfiguration(data) {
    //TODO: All the final styling is still needed
    if (data.positive_discharges == undefined ||
        data.district_average == undefined ||
        data.state_average == undefined) {
            let msg = `Unable to generate a PO Comparison chart because required`
                + ` properties do not exist. full data = ${JSON.stringify(data)}`;
            throw new TypeError(msg);
    }

    const chartData = [data.positive_discharges, data.district_average, data.state_average];
    const configuration = {
        type: 'horizontalBar',
        data: {
            labels: ['You', 'District average', 'State average'],
            datasets: [{
                label: 'PO comparison',
                data: chartData,
                backgroundColor: [
                    COLORS[0],
                    COLORS[1],
                    COLORS[2]
                ],
            }]
        },
        options: {
            scales: {
                xAxes: [{
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    };
    return configuration;
}

/**
 * Generates a chart image using the supplied configuration object.
 * @param {Object} configuration - A valid chartjs configuration object
 * @returns {Object} A buffer containing the image of the chart.
 */
async function createChartImage(configuration) {
    const { CanvasRenderService } = require('chartjs-node-canvas');
    //TODO: Test values for now.  Need production size
    const width = 400;
    const height = 400;
    const chartCallback = (ChartJS) => {
        //TODO: Set the background color of the chart to white so we can see it
        // easier during testing.  We'll likely remove this later in favor of
        // background set in the HTML.
        ChartJS.plugins.register({
            beforeDraw: (chart, options) => {
                const ctx = chart.ctx;
                ctx.fillStyle = '#FFFFFF';
                ctx.fillRect(0, 0, width, height);
            }
        });
    };

    const canvasRenderService = new CanvasRenderService(width, height, chartCallback);
    const image = await canvasRenderService.renderToBuffer(configuration);
    return image;
}

/**
 * Given a buffer, writes to Cloud Storage
 * @param {Object} image - A buffer containing a PNG image
 */
function writeImageToBucket(image, bucketName, objectName) {
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(objectName);

    const stream = file.createWriteStream({
        metadata: {
            contentType: 'image/png'
        }
    });

    stream.on('error', (err) => {
        console.log(err);
    });

    stream.on('finish', () => {
        //file.makePublic();
    });

    stream.end(image);
}

/**
 * Sends a Pub/Sub message to the email generation topic along with the
 * recipient data.
 * @param {String} data - A JSON string containing all of the recipient's data
*/
async function sendMessageToGeneration(data) {
    const pubsub = new PubSub();
    const topicName = `projects/${process.env.GCP_PROJECT}/topics/report_generate_email_for_recipient`;
    const dataBuffer = Buffer.from(data);
    await pubsub.topic(topicName).publish(dataBuffer);
}
