const Apify = require('apify');

const { log } = Apify.utils;

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
        handlePageTimeoutSecs: 120, // page randomly fails to respond
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
            });
        },
        handlePageFunction: async ({ page }) => {
            await page.evaluate(() => {
                window.dashboard.coronavirus.brazilCSV();
            });
        }
    });

    await crawler.run();

    if (!csvData[0]) {
        throw new Error('Missing data');
    }

    const [modified] = csvData.pop();

    if (!modified) {
        throw new Error('Missing modified');
    }

    const [, day, time] = modified.match(/em ([\S]+) às ([\S]+)/);
    const lastUpdatedAtSource = new Date(`${day.split('/').reverse().join('-')}T${time}:00-03:00`).toISOString();

    const regions = csvData.filter(i => /^Unidade/.test(i[0]));

    const cleanNumber = (strVal) => +(strVal.replace(/[^\d]+/g, ''));

    const countTotals = (index) => {
        return regions.reduce((out, i) => (out + cleanNumber(i[index])), 0)
    }

    const extractState = (val) => {
        const matches = val.match(/\(([A-Z]{2})\)/);

        if (matches && matches[1]) {
            return matches[1]
        }

        return val;
    }

    const totalTested = countTotals(2);
    const infected = countTotals(4);
    const testedNotInfected = countTotals(6);
    const deceased = countTotals(8);

    const data = {
        totalTested,
        testedNotInfected,
        infected,
        deceased,
        infectedByRegion: regions.map(s => ({ state: extractState(s[1]), count: cleanNumber(s[4]) })),
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

    log.info('Done');
});
