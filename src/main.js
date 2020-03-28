const Apify = require('apify');

const { log } = Apify.utils;

Apify.main(async () => {
    const kv = await Apify.openKeyValueStore('COVID-19-BRAZIL');
    const history = await Apify.openDataset('COVID-19-BRAZIL-HISTORY');

    const sourceUrl = 'https://www.saude.gov.br/noticias/agencia-saude';

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

    const requestList = await Apify.openRequestList('mapa', [{
        url: `${process.env.BASE_URL}/prod/PortalMapa`,
        userData: {
            LABEL: 'regions',
        },
    }, {
        url: `${process.env.BASE_URL}/prod/PortalGeral`,
        userData: {
            LABEL: 'geral',
        },
    }]);

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

    const version = 3;
    const data = {
        version,

    };
    let lastUpdatedAtSource;

    const crawler = new Apify.CheerioCrawler({
        requestList,
        additionalMimeTypes: ['application/json'],
        useSessionPool: true,
        maxConcurrency: 1,
        useApifyProxy: true,
        prepareRequestFunction: ({ request }) => {
            request.headers.Referer = 'https://covid.saude.gov.br/';
            request.headers['X-Parse-Application-Id'] = process.env.KEY;
        },
        handlePageTimeoutSecs: 180,
        handlePageFunction: async ({ request, json }) => {
            const { results } = json;
            const { LABEL } = request.userData;

            if (!results || !results[0]) {
                await Apify.setValue(`results-${Math.random()}`, { results });
                throw new Error('Results are empty');
            }

            if (LABEL === 'regions') {

            } else if (LABEL === 'geral') {
                const dateModified = new Date(results[0].updatedAt);

                if (Number.isNaN(dateModified.getTime())) {
                    log.warning('Invalid date', { dateModified, results });

                    throw new Error('Invalid date');
                }

                if (dateModified.getTime() < lastUpdate.getTime()) {
                    return;
                }

                if (!lastUpdatedAtSource || dateModified.getTime() > lastUpdatedAtSource.getTime()) {
                    lastUpdatedAtSource = dateModified;
                }
            }


            // if (Number.isNaN(dateModified.getTime())) {
            //     log.warning('Invalid date', { dateModified, ld });

            //     throw new Error('Invalid date');
            // }

            // if (dateModified.getTime() < lastUpdate.getTime()) {
            //     return;
            // }

            // if (!lastUpdatedAtSource || dateModified.getTime() > lastUpdatedAtSource.getTime()) {
            //     lastUpdatedAtSource = new Date(dateModified);
            // }

            // const aggregate = [];

            // $('.su-table table tr').each((index, el) => {
            //     const $el = $(el);
            //     const tds = $el.find('td');

            //     if (tds.length !== 5) {
            //         return;
            //     }

            //     const state = tds.eq(DATA_INDEX.UF).text().trim();
            //     const deceased = +(tds.eq(DATA_INDEX.DEATHS).text().trim());
            //     const infected = +(tds.eq(DATA_INDEX.INFECTED).text().trim());

            //     aggregate.push({ state, deceased, infected });
            // });

            // if (aggregate.length) {
            //     data.set(dateModified.toISOString(), aggregate);
            // }
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

    await kv.setValue('LATEST', transformedData[transformedData.length - 1]);

    lastUpdatedAtSource = order.pop();

    if (lastUpdate.toISOString() !== lastUpdatedAtSource) {
        await history.pushData(transformedData);
    }

    // always push data to default dataset
    await Apify.pushData(transformedData);

    log.info('Done');
});
