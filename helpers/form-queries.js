const uuid = require('uuid');

const formUpdateQueryWithAddInListScript = (index, id, field, newItem) => {
  const updateStr = `{"update": {"_id": "${id}", "_index": "${index}", "retry_on_conflict" : 3}}`;
  const scriptStr = `{"script": {"lang" : "painless","source": """if(ctx._source.${field} instanceof String ) { ctx._source.${field} = [ctx._source.${field}, params.newItem ] } else if ((ctx._source.${field} instanceof List)) { ctx._source.${field}.add(params.newItem) } else { ctx._source.${field} = [params.newItem] }""","params" : {"newItem" : "${newItem}"}}}`;
  return updateStr + '\n' + scriptStr;
};

const formUpdateQueryDocumentOrInsertOne = (index, id, object) => {
  const updateStr = `{"update": {"_id": "${id || uuid.v4()}", "_index": "${index}","_type": "_doc"}}`;
  const docStr = `{"doc":${JSON.stringify(object)},"doc_as_upsert": "true"}`;
  return updateStr + '\n' + docStr;
};

const formCreateQueryWithInsert = (index, id, object) => {
  const createStr = `{"create": {"_id": "${id}", "_index": "${index}"}}`;
  const docStr = JSON.stringify(object);
  return createStr + '\n' + docStr;
};

const formDeleteQueryById = (index, id) => {
  return `{ "delete": { "_index": "${index}", "_id": "${id}" } }`;
};

const formGetQueryByField = (field, value) => ({
  query: {
    match: {
      [field]: value
    }
  }
});

module.exports = {
  formGetQueryByField,
  formDeleteQueryById,
  formCreateQueryWithInsert,
  formUpdateQueryWithAddInListScript,
  formUpdateQueryDocumentOrInsertOne
};
