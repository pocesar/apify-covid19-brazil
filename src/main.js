const Apify = require('apify');
const cheerio = require('cheerio');

const { log } = Apify.utils;

Apify.main(async () => {
    const kv = await Apify.openKeyValueStore('COVID-19-BRAZIL');
    const history = await Apify.openDataset('COVID-19-BRAZIL-HISTORY');

    const sourceUrl = 'https://www.saude.gov.br/noticias/agencia-saude';

    const requestQueue = await Apify.openRequestQueue();
    const info = await history.getInfo();
    let lastUpdate = new Date();

    if (info && info.itemCount > 0) {
        const currentData = await history.getData({
            limit: 1,
            offset: info.itemCount - 1,
        });

        if (currentData && currentData.items[0] && currentData.items[0].lastUpdatedAtSource) {
            lastUpdate = new Date(currentData.items[0].lastUpdatedAtSource);
        }
    }

    await requestQueue.addRequest({
        url: 'https://www.saude.gov.br/noticias/agencia-saude?format=feed&type=rss',
        uniqueKey: `${Math.random()}`,
        userData: {
            label: 'FEED',
        },
    });

    /**
     * @param {any[]} values
     * @param {'infected'|'deceased'} key
     */
    const countTotals = (values, key) => {
        return values.reduce((out, i) => (out + (i[key] || 0)), 0);
    };

    /**
     * @param {any[]} values
     * @param {'deceased'|'infected'} key
     */
    const mapRegions = (values, key) => {
        return values.map(s => ({ state: s.state, count: s[key] || 0 }));
    };

    const cleanNumber = (str) => str.replace(/[^0-9]+/, '');

    const DATA_INDEX = {
        UF: 1,
        INFECTED: 2,
        DEATHS: 3,
    };

    const version = 2;
    const data = new Map();
    let lastUpdatedAtSource;

    const crawler = new Apify.CheerioCrawler({
        requestQueue,
        additionalMimeTypes: ['application/rss+xml'],
        useSessionPool: true,
        maxConcurrency: 1,
        useApifyProxy: true,
        handlePageTimeoutSecs: 180,
        handlePageFunction: async ({ request, body, $ }) => {
            const { label } = request.userData;

            if (label === 'FEED') {
                log.info('Parsing feed');

                $ = cheerio.load(body, { decodeEntities: true, xmlMode: true });

                const urls = new Set();

                $('item').each((index, el) => {
                    const $el = $(el);

                    const link = $el.find('link').text();

                    if (link && link.includes('confirmados') && link.includes('coronavirus')) {
                        urls.add(link);
                    }
                });

                for (const url of urls) {
                    await requestQueue.addRequest({
                        url,
                        userData: {
                            label: 'PAGE',
                        },
                    });
                }

                log.info(`Found ${urls.size} new possible urls`);
            } else if (label === 'PAGE') {
                const ld = JSON.parse($('[type="application/ld+json"]').html());

                const dateModified = new Date(ld.dateModified);

                if (Number.isNaN(dateModified.getTime())) {
                    log.warning('Invalid date', { dateModified, ld });

                    throw new Error('Invalid date');
                }

                if (dateModified.getTime() < lastUpdate.getTime()) {
                    return;
                }

                if (!lastUpdatedAtSource || dateModified.getTime() > lastUpdatedAtSource.getTime()) {
                    lastUpdatedAtSource = new Date(dateModified);
                }

                const aggregate = [];

                $('.su-table table tr').each((index, el) => {
                    const $el = $(el);
                    const tds = $el.find('td:not([colspan])');

                    if (tds.length !== 5) {
                        return;
                    }

                    const state = tds.eq(DATA_INDEX.UF).text().trim();
                    const deceased = +(cleanNumber(tds.eq(DATA_INDEX.DEATHS).text().trim()));
                    const infected = +(cleanNumber(tds.eq(DATA_INDEX.INFECTED).text().trim()));

                    aggregate.push({ state, deceased, infected });
                });

                if (aggregate.length) {
                    data.set(dateModified.toISOString(), aggregate);
                }
            }
        },
        handleFailedRequestFunction: ({ error }) => {
            log.exception(error, 'Failed after all retries');
        },
    });

    await crawler.run();

    if (!data.size || !lastUpdatedAtSource) {
        throw new Error('Missing data');
    }

    const order = [...data.keys()].sort((a, b) => {
        const dateA = new Date(a).getTime();
        const dateB = new Date(b).getTime();

        return dateA - dateB;
    });

    const transformedData = order.map((key) => {
        const set = data.get(key);

        return {
            version,
            infected: countTotals(set, 'infected'),
            deceased: countTotals(set, 'deceased'),
            infectedByRegion: mapRegions(set, 'infected'),
            deceasedByRegion: mapRegions(set, 'deceased'),
            sourceUrl,
            lastUpdatedAtSource: key,
            lastUpdatedAtApify: new Date().toISOString(),
            readMe: 'https://apify.com/pocesar/covid-brazil',
        };
    });

    const checkRegions = (item) => !Number.isInteger(item.count) || !item.state || item.state.length !== 2;

    // sanity check before updating, the data is seldomly unreliable
    for (const item of transformedData) {
        if (!Number.isInteger(item.deceased)
            || !Number.isInteger(item.infected)
            || !item.infected
            || !item.deceased
            || item.deceasedByRegion.length !== 27
            || item.infectedByRegion.length !== 27
            || item.deceasedByRegion.some(checkRegions)
            || item.infectedByRegion.some(checkRegions)
        ) {
            await Apify.setValue('transformedData', transformedData);

            throw new Error('Data check failed');
        }
    }

    await kv.setValue('LATEST', transformedData[transformedData.length - 1]);

    lastUpdatedAtSource = order.pop();

    if (lastUpdate.toISOString() !== lastUpdatedAtSource) {
        await history.pushData(transformedData);
    }

    // always push data to default dataset
    await Apify.pushData(transformedData);

    log.info('Done');
});
