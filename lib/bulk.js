const _ = require('lodash');
const log = require('debug')('loopback:connector:elasticsearch');

function bulk(model, data, callback) {
    const self = this;
    if (self.debug) {
        log('ESConnector.prototype.bulk', 'model', model);
    }
    const idName = self.idName(model);
    log('ESConnector.prototype.bulk', 'idName', idName);

    const defaults = self.addDefaults(model, 'bulk');
    const reqBody = [];

    _.forEach(data, (item, key) => {
        const op = [];
        op.push(JSON.stringify({update: {_id: item['data'][idName], _index: self.db.index, _type: '_doc', retry_on_conflict: 3}}));
        op.push(JSON.stringify({
            script: {
                source: Object.keys(item['params']).map(p => {return `ctx._source.${p} = ctx._source.${p} += params.${p};`}).join(''),
                lang: "painless",
                params: item['params'],
            },
            upsert: item['data']
        }));
        reqBody.push(op.join("\n"));
    });

    const document = _.defaults({
        body: reqBody.join("\n") + "\n"
    }, defaults);
    log('ESConnector.prototype.bulk', 'document to update', document);

    self.db.bulk(document)
        .then(({ body }) => {
            log('ESConnector.prototype.bulk', 'response', body);
            return callback(null, {
                errors: body.errors
            });
        }).catch((error) => {
        log('ESConnector.prototype.updateAll', error.message);
        return callback(error);
    });
}

module.exports.bulk = bulk;
