var ratebeer = require('ratebeer');
var async = require('async');
var inquirer = require('inquirer');
var _ = require('underscore');
var ss = require('string-similarity');
var skuLinkRepo = require('./sku_link_repo.json');
var fs = require('fs');

function sortByBestMatch(term, options) {
  return _.sortBy(options, function(option) {
    return ss.compareTwoStrings(term, option.name);
  }).reverse();
}

function addSKULink(sku,url) {
  skuLinkRepo[sku] = url;
  fs.writeFile(__dirname + '/sku_link_repo.json', JSON.stringify(skuLinkRepo), function(){});
};

module.exports = function fetchRatebeerURL(db, product, callback) {
  if (skuLinkRepo[product.sku]) return callback(null, skuLinkRepo[product.sku]);
  db.query("MATCH (b:beer { sku: {sku} }) RETURN b.ratebeerUrl as url", {sku:product.sku}, function(err, result) {
    if (err) return callback(err);
    else if (result != null && result[0] != null && result[0].url != null) return callback(null, result[0].url);
    else queuedFindEntry({db:db,product:product}, callback);
  });
};

var findBeerQueue = async.queue(findEntry, 15);
var queuedFindEntry = findBeerQueue.push.bind(findBeerQueue)

function findEntry(args, callback) {
  _findEntry(args.product.manufacturer + ' ' + args.product.title, function(err, result) {
    callback(err,result);
    if (result) args.db.query("merge (b:beer { sku:{sku} }) set b.ratebeerUrl = {url}", {sku:args.product.sku, url:result}, function() {});
    addSKULink(args.product.sku, result);
  }, args.product.title);
};

var promptQueue = async.queue(function(opts, cb) {
  _.defer(function() { inquirer.prompt(opts,cb); });
}, 1);
var queuePrompt = promptQueue.push.bind(promptQueue);
var nextPrompt = promptQueue.unshift.bind(promptQueue);
function _findEntry(searchTerm, callback, secondary, promptQueuer) {
  var prompt = promptQueuer || queuePrompt;
  ratebeer.searchAll(searchTerm, function(err, results) {
    if (err) return callback(err);
    if (results.length > 0) {
      var lowerCaseFirstResult = results[0].name.toLowerCase();
      var lowerCaseSearch = searchTerm.toLowerCase();
      var lowerCaseSecondary = secondary && secondary.toLowerCase();
      if (lowerCaseFirstResult == lowerCaseSearch || lowerCaseFirstResult === lowerCaseSecondary) {
        console.log('auto-matched to a primary or secondary:', results[0].name);
        return callback(null, results[0].url);
      }
    }
    if (results.length == 1) {
      prompt([{
        type: 'expand',
        name: 'answer',
        message: 'Found 1 beer, is this right for "' + searchTerm + '"?: "' + results[0].name + '"',
        choices: [{key:'y', name:'yes', value:true},
                  {key:'s', name:'skip this beer', value:false},
                  {key:'a', name:'amend search query', value:'amend'}],
        default: 0
      }], function(result) {
        if (result.answer === true) callback(null, results[0].url)
        else if (result.answer === false) callback();
        else  findEntryWithCustomSearchTerm(searchTerm, callback);
      });
    } else if (results.length > 1) {
      var selections = sortByBestMatch(searchTerm, results).map(function(result) {
        return {
          name: result.name,
          value: result
        }
      });
      selections.push({name:'-> Skip this beer', value: 'skip'});
      selections.push({name:'-> Amend search query', value: 'amend'});
      prompt([{
        type: 'rawlist',
        name: 'selected',
        message: 'Select the correct beer for "' + searchTerm + '"',
        choices: selections,
        default: 0
      }], function(result) {
        if (result.selected == 'skip') callback();
        else if (result.selected == 'amend') findEntryWithCustomSearchTerm(searchTerm, callback);
        else callback(null, result.selected.url)
      });
    } else {
      if (secondary) return _findEntry(secondary, callback);
      prompt([{
        type:'confirm',
        name: 'proceed',
        message: 'No results for "' + searchTerm + '". Amend search?',
        default: true
      }], function(result) {
        if (result.proceed) findEntryWithCustomSearchTerm(searchTerm, callback);
        else callback();
      });
    }
  });
};

function findEntryWithCustomSearchTerm(searchTerm, callback) {
  nextPrompt([{
    type: 'input',
    name: 'term',
    message: 'Input a new search term for this beer',
    default: searchTerm
  }], function(result) {
    _findEntry(result.term, callback, null, nextPrompt);
  });
}
