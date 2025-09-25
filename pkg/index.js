const bindings = require('./asset360_rust.js');

async function init() {
  return Promise.resolve();
}

async function loadSchemaViewAsync(yaml) {
  await init();
  return bindings.loadSchemaView(yaml);
}

module.exports = {
  ...bindings,
  init,
  loadSchemaViewAsync,
};
