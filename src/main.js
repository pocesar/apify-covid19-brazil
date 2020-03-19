const Apify = require('apify');

const { log, requestAsBrowser } = Apify.utils;

Apify.main(async () => {
    const kv = await Apify.openKeyValueStore('COVID-19-BRAZIL');
    const history = await Apify.openDataset('COVID-19-BRAZIL-HISTORY');

    const sourceUrl = 'http://plataforma.saude.gov.br/novocoronavirus/';

    const requestList = await Apify.openRequestList('page', [
        `http://plataforma.saude.gov.br/novocoronavirus/resources/scripts/database.js?v=${Math.round(Date.now() / 1000)}`,
    ]);

    /**
     * @param {any[]} values
     * @param {'suspects'|'cases'|'refuses'|'deaths'} key
     */
    const countTotals = (values, key) => {
        return values.reduce((out, i) => (out + (i[key] || 0)), 0);
    };
    const statesMap = {
        11: 'RO',
        12: 'AC',
        13: 'AM',
        14: 'RR',
        15: 'PA',
        16: 'AP',
        17: 'TO',
        21: 'MA',
        22: 'PI',
        23: 'CE',
        24: 'RN',
        25: 'PB',
        26: 'PE',
        27: 'AL',
        28: 'SE',
        29: 'BA',
        31: 'MG',
        32: 'ES',
        33: 'RJ',
        35: 'SP',
        41: 'PR',
        42: 'SC',
        43: 'RS',
        50: 'MS',
        51: 'MT',
        52: 'GO',
        53: 'DF',
    };

    /**
     * @param {any[]} values
     * @param {'suspects'|'cases'|'refuses'|'deaths'} key
     */
    const mapRegions = (values, key) => {
        return values.map(i => ({
            state: statesMap[i.uid],
            count: i[key] || 0,
        }));
    };

    let data;
    let lastUpdatedAtSource;

    const crawler = new Apify.BasicCrawler({
        requestList,
        useSessionPool: true,
        maxConcurrency: 1,
        handleRequestTimeoutSecs: 180,
        handleRequestFunction: async ({ request, session }) => {
            const result = await requestAsBrowser({
                url: request.url,
                method: 'GET',
                abortFunction: () => false,
                gzip: true,
                useBrotli: false,
                timeoutSecs: 120,
                json: false,
                headers: {
                    Accept: '*/*',
                    Pragma: 'no-cache',
                    DNT: '1',
                    'Cache-Control': 'no-cache',
                    Referer: sourceUrl,
                },
                proxyUrl: Apify.isAtHome() ? Apify.getApifyProxyUrl({
                    session: session.id,
                }) : undefined,
            });

            if (result.statusCode !== 200) {
                await Apify.setValue(`statusCode-${result.statusCode}-${Math.random()}`, result.body, { contentType: 'text/plain' });

                throw new Error(`Status code ${result.statusCode}`);
            }

            const searchString = 'var database=';

            if (!result.body || !result.body.includes(searchString)) {
                await Apify.setValue(`body-${Math.random()}`, result.body, { contentType: 'text/plain' });

                throw new Error('Invalid body received');
            }

            const { body } = result;

            const startOffset = body.indexOf(searchString) + searchString.length;
            const endOffset = body.lastIndexOf('}') + 1;
            const slice = body.slice(startOffset, endOffset);

            try {
                const jsonData = JSON.parse(slice).brazil.pop();
                const { values, date, time } = jsonData;

                lastUpdatedAtSource = new Date(`${date.split('/').reverse().join('-')}T${time}:00-03:00`).toISOString();

                data = {
                    suspiciousCases: countTotals(values, 'suspects'),
                    testedNotInfected: countTotals(values, 'refuses'),
                    infected: countTotals(values, 'cases'),
                    deceased: countTotals(values, 'deaths'),
                    suspiciousCasesByRegion: mapRegions(values, 'suspects'),
                    testedNotInfectedByRegion: mapRegions(values, 'refuses'),
                    infectedByRegion: mapRegions(values, 'cases'),
                    deceasedByRegion: mapRegions(values, 'deaths'),
                    sourceUrl,
                    lastUpdatedAtSource,
                    lastUpdatedAtApify: new Date().toISOString(),
                    readMe: 'https://apify.com/pocesar/covid-brazil',
                };
            } catch (e) {
                await Apify.setValue(`json-${Math.random()}`, slice, { contentType: 'text/plain' });

                log.exception(e);

                throw e;
            }
        },
        handleFailedRequestFunction: ({ error }) => {
            log.exception(error, 'Failed after all retries');
        },
    });

    await crawler.run();

    if (!data || !lastUpdatedAtSource) {
        throw new Error('Missing data');
    }

    // we want to crash here if there's something wrong with the data
    for (const key of ['suspiciousCasesByRegion', 'testedNotInfectedByRegion', 'infectedByRegion', 'deceasedByRegion']) {
        if (!data[key]
        || data[key].length === 0
        || data[key].some(i => (typeof i.count !== 'number' || !i.state || !/^[A-Z]{2}$/i.test(i.state)))
        ) {
            await Apify.setValue(`data-${Math.random()}`, data);

            throw new Error('Data seems corrupt');
        }
    }

    await kv.setValue('LATEST', data);

    const info = await history.getInfo();

    if (info && info.itemCount > 0) {
        const currentData = await history.getData({
            limit: 1,
            offset: info.itemCount - 1,
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
