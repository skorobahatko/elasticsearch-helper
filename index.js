const fs = require('fs');
const { formGetQueryByField, formDeleteQueryById, httpRequest, curlRequest } = require('./helpers');

const arrayIntoChunks = (array, size) => {
  const chunkedArr = [];
  const copied = [...array];
  const numOfChild = Math.ceil(copied.length / size);
  for (let i = 0; i < numOfChild; i += 1) {
    chunkedArr.push(copied.splice(0, size));
  }

  return chunkedArr;
};

const ELASTIC_SEARCH_URL = 'URL';

const removeDuplicates = async (bulkQuery) => {
  // Splitting for chunks
  const splittedQueries = bulkQuery.split('\n');

  const chunks = arrayIntoChunks(splittedQueries, 100);

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];

    // Replacing useless single quotes
    const regex = new RegExp(/'/g);
    const str = chunk.join('\n').replace(regex, '', 'g');

    // Took time for execute query in elastic
    let took;
    try {
      const result = await curlRequest('POST', `${ELASTIC_SEARCH_URL}/_bulk?pretty`, str);

      try {
        took = JSON.parse(result)?.took;
      } catch (err) {
        took = 'Error';
      }
    } catch (err) {
      console.log(err);
      console.log(`Failed in chunk, ${i}`);
    }

    console.log(`Chunk ${i}, Took time: ${took}`);

    await delay(500);
  }
};

const findDuplicates = async () => {
  // Enter your index and field name by what we search for duplicates
  const index = 'INDEX';
  const field = 'FIELD';

  // Requires array values of field
  const valuesForDiff = JSON.parse(fs.readFileSync('FILENAME', { encoding: 'utf-8' }));

  // Query variable will contains all formed queries by search for duplicates
  let bulkQuery;
  // Failed requests will be saved in separated file for debug
  const failedRequests = [];

  // Splitting for chunks for reduce load on cluster
  const chunks = arrayIntoChunks(valuesForDiff, 50);

  for (const chunk of chunks) {
    // Fetching queries for each value
    const responses = await Promise.allSettled(
      chunk.map(async (i) => ({
        [field]: i,
        value: await httpRequest('GET', `${ELASTIC_SEARCH_URL}/${index}/_search`, formGetQueryByField(field, i))
      }))
    );

    const failed = responses.map((i) => i.status === 'rejected' && i.reason).filter(Boolean);
    failedRequests.push(...failed);

    const successed = responses.map((i) => i.status === 'fulfilled' && i.value).filter(Boolean);

    const idsForDelete = [];

    let counter = 0;
    // Iterating values and getting items for delete
    for (const res of successed) {
      counter++;
      if (!res.value?.data?.hits?.hits?.length) continue;

      const correctItems = res.value?.data?.hits.hits.filter((curr) => curr._source[field] === res[field]);

      if (correctItems.length <= 1) {
        console.log(`Correct item is single or not exists`);
        continue;
      }

      // Sorting items to find last updated and save it
      correctItems.sort((a, b) => b._source.updatedAt - a._source.marketCapLastUpdatedAtISO);

      const lastItem = correctItems[0];
      const deleteIds = correctItems.map((i) => i?._id && i?._id !== lastItem._id).filter(Boolean);

      idsForDelete.push(...deleteIds);
    }

    bulkQuery += `${idsForDelete.map((id) => formDeleteQueryById(index, id)).join('\n')}`;

    console.log(`Chunk: ${counter}. Result: delete - ${idsForDelete.length}`);

    // Delay for reduce load on cluster
    await delay(500);
  }

  return bulkQuery;
};

const startRemovingDuplicates = async () => {
  const query = await findDuplicates();

  if (!query) throw new Error('Query is not formed');

  await removeDuplicates(query);

  console.log('Duplicates successfully removed');
};
