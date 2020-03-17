const Apify = require('apify');

const { log, sleep } = Apify.utils;

Apify.main(async () => {
    const kv = await Apify.openKeyValueStore('COVID-19-BRAZIL');
    const history = await Apify.openDataset('COVID-19-BRAZIL-HISTORY');

    const sourceUrl = 'http://plataforma.saude.gov.br/novocoronavirus/';

    const requestList = await Apify.openRequestList('page', [
        sourceUrl
    ]);

    const csvData = [];

    const crawler = new Apify.PuppeteerCrawler({
        requestList,
        launchPuppeteerOptions: {
            useApifyProxy: Apify.isAtHome(),
        },
        handlePageTimeoutSecs: 180, // page randomly fails to respond
        gotoFunction: async ({ page, request  }) => {
            const functionName = `fn${(Math.random() * 1000).toFixed(0)}`
            await page.exposeFunction(functionName, (data) => {
                csvData.push(...data);
            });

            await page.evaluateOnNewDocument((fName) => {
                window.addEventListener('load', () => {
                    window.dashboard.toolbox.exportCSV = window[fName];
                });
            }, functionName);

            return page.goto(request.url, {
                waitUntil: 'networkidle0',
                timeout: 180 * 1000
            });
        },
        handlePageFunction: async ({ page, response }) => {
            if (response.status() !== 200) {
                await sleep(30000);

                throw new Error(`Status code ${response.status()}`);
            }

            await page.evaluate(() => {
                window.dashboard.coronavirus.brazilCSV();
            });
        }
    });

    await crawler.run();

    if (!csvData[0]) {
        await Apify.setValue('missing', csvData);

        throw new Error('Missing data');
    }

    const [modified] = csvData.pop();

    if (!modified) {
        await Apify.setValue('modified', csvData);

        throw new Error('Missing modified');
    }

    const [, day, time] = modified.match(/em ([\S]+) às ([\S]+)/);
    const lastUpdatedAtSource = new Date(`${day.split('/').reverse().join('-')}T${time}:00-03:00`).toISOString();

    const regions = csvData.filter(i => /^Unidade/.test(i[0]));

    if (!regions.length || regions.some(s => (!s[1] || s[1].includes('undefined')))) {
        await Apify.setValue('regions', regions);

        throw new Error('Data seems corrupt');
    }

    const cleanNumber = (strVal) => +(strVal.replace(/[^\d]+/g, ''));

    const DATA_INDEX = {
        TESTED: 2,
        INFECTED: 4,
        NOT_INFECTED: 6,
        DECEASED: 8
    };

    const countTotals = (index) => {
        return regions.reduce((out, i) => (out + cleanNumber(i[index])), 0)
    }

    const extractState = (val) => {
        const matches = val.match(/\(([A-Z]{2})\)/);

        if (matches && matches[1]) {
            return matches[1]
        }

        throw new Error('extractState seems corrupt');
    }

    const countRegion = (index) => {
        return regions.map(s => ({ state: extractState(s[1]), count: cleanNumber(s[index]) }));
    }

    const totalTested = countTotals(DATA_INDEX.TESTED);
    const infected = countTotals(DATA_INDEX.INFECTED);
    const testedNotInfected = countTotals(DATA_INDEX.NOT_INFECTED);
    const deceased = countTotals(DATA_INDEX.DECEASED);

    let byRegion = {};

    try {
        byRegion = {
            testedByRegion: countRegion(DATA_INDEX.TESTED),
            testedNotInfectedByRegion: countRegion(DATA_INDEX.NOT_INFECTED),
            infectedByRegion: countRegion(DATA_INDEX.INFECTED),
            deceasedByRegion: countRegion(DATA_INDEX.DECEASED),
        }
    } catch (e) {
        // ugly hack, we need to catch corrupt data from the regions to analyze...
        // usually the page failed to load completely
        await Apify.setValue('countRegion', regions);

        throw e;
    }

    const data = {
        totalTested,
        testedNotInfected,
        infected,
        deceased,
        ...byRegion,
        sourceUrl,
        lastUpdatedAtSource,
        lastUpdatedAtApify: new Date().toISOString(),
        readMe: 'https://apify.com/pocesar/covid-brazil'
    };

    await kv.setValue('LATEST', data);

    const info = await history.getInfo();

    if (info && info.itemCount > 0) {
        const currentData = await history.getData({
            limit: 1,
            offset: info.itemCount - 1
        });

        if (currentData && currentData.items[0] && currentData.items[0].lastUpdatedAtSource !== lastUpdatedAtSource) {
            await history.pushData(data);
        }
    } else {
        await history.pushData(data);
    }

    // always push data to default dataset
    await Apify.pushData(data);

    log.info('Done');
});
