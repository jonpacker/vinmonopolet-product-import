var vinmonopolet = require('vinmonopolet');
var db = require('seraph')();
var _ = require('underscore');
var augur = require('augur');
var async = require('async');
var ratebeer = require('ratebeer');
var findCorrectRatebeerEntry = require('./match_ratebeer');

var txn = db.batch();
var productCount = 0;
var ops = [];
var secondary = db.batch();
var ratebeerQueryCounts = 0;
var vmQueryCounts = 0;

vinmonopolet.getProductStream().on('data', function(product) {
  if (product.productType != 'Øl') return;
  productCount++
  var query = 'MERGE (beer:beer {sku: {product}.sku}) ';
  
  if (product.manufacturer) {
    query += 'MERGE (brewery:brewery { name: {product}.manufacturer })';
    query += 'MERGE brewery<-[:brewed_by]-beer ';
  }
  if (product.country) {
    query += 'MERGE (country:country { name: {product}.country }) ';
    query += 'MERGE beer-[:brewed_in]->country ';
  }
  if (product.region) {
    query += 'MERGE (region:region { name: {product}.region})<-[:has_region]-country ';
    query += 'MERGE beer-[:brewed_in]->region ';
  }
  if (product.subregion) {
    query += 'MERGE (subregion:subregion { name: {product}.subRegion })<-[:has_subregion]-region ';
    query += 'MERGE beer-[:brewed_in]->subregion ';
  }
  if (product.wholesaler) {
    query += 'MERGE (wholesaler:wholesaler { name: {product}.wholesaler }) ';
    query += 'MERGE beer-[:sold_by]->wholesaler ';
  }
  if (product.distributor) {
    query += 'MERGE (distributor:distributor { name: {product}.distributor }) ';
    query += 'MERGE beer-[:distributed_by]->distributor ';
  }
  if (product.productSelection) {
    query += 'MERGE (productSelection:productSelection { name: {product}.productSelection }) ';
    query += 'MERGE beer-[:in_selection]->productSelection ';
  }
  if (product.storeCategory) {
    query += 'MERGE (storeCategory:storeCategory { name: {product}.storeCategory }) ';
    query += 'MERGE beer-[:in_category]->storeCategory ';
  }
  if (product.foodPairings) {
    product.foodPairings.forEach(function(pairing, idx) {
      query += 'MERGE (food' + idx + ':food { name: {product}.foodPairings[' + idx + '] }) ';
      query += 'MERGE beer-[:pairs_with]->food' + idx + ' ';
    });
  }
  
  var stripped = _.omit(product, ['manufacturer', 'country', 'region', 'subRegion', 'wholesaler', 'distributor', 'foodPairings']);

  Object.keys(stripped).forEach(function(key) {
    query += 'SET beer.' + key + ' = {stripped}.' + key + ' ';
  });

  query += 'RETURN beer.sku';
  
  txn.query(query, {product:product, stripped:stripped});
  
  var fetchAvailability = augur();
  ops.push(fetchAvailability);
  

  var timeout = false;
  setTimeout(function() { if (!timeout) fetchAvailability()},120000);
  vinmonopolet.getProduct(product.sku, function(err, product) {
    timeout = true;
    if (err || !product) return fetchAvailability();
    
    vmQueryCounts++;
    if (vmQueryCounts % 100 == 0) console.log("Finished", vmQueryCounts, "getProduct calls");

    var query = "MATCH (beer:beer { sku: {product}.sku }) ";
    query += " OPTIONAL MATCH (:store)<-[old_stock_rel:in_stock]-beer ";
    query += " DELETE old_stock_rel ";
    secondary.query(query, {product:product});
    query = "MATCH (beer:beer { sku: {product}.sku }) ";
    if (product.availability) {
      product.availability.forEach(function(store, idx) {
        query += ' MERGE (store'+idx+':store { name: {product}.availabilityStoreName['+idx+'], storeId: {product}.availabilityStoreId['+idx+'] })';
        query += ' MERGE store'+idx+'<-[:in_stock { quantity: {product}.availabilityQuantity['+idx+'] }]-beer ';
      });
      product.availabilityStoreName = _.pluck(product.availability, 'storeName')
      product.availabilityStoreId = _.pluck(product.availability, 'storeId')
      product.availabilityQuantity = _.pluck(product.availability, 'quantity')
    }
  
    secondary.query(query, {product:product});
    fetchAvailability()
  });
  
  var fetchRatebeerMetadata = augur();
  ops.push(fetchRatebeerMetadata);
  findCorrectRatebeerEntry(db,product, function(err, res) {
    if (err || !res) return fetchRatebeerMetadata();
    ratebeer.getBeerByUrl(res,function(err, beer) {
      if (err || !beer) fetchRatebeerMetadata();
      else {
        beer.url = res;
        processRatebeerMetadata(beer);
      }
    });
  });
  var processRatebeerMetadata = function(beer) {
    ratebeerQueryCounts++;
    if (ratebeerQueryCounts % 100 == 0) console.log("Finished", ratebeerQueryCounts, "ratebeer queries");
    if (!beer) return fetchRatebeerMetadata();
    var query = "MATCH (beer:beer { sku: {product}.sku }) ";
    var sets = [beer.url ? "beer.ratebeerUrl = {rb}.url" : ""];
    if (beer.ratingOverall != null) {
      sets.push("beer.ratebeerRatingOverall = {rb}.ratingOverall");
      sets.push("beer.ratebeerRatingStyle = {rb}.ratingStyle");
    }
    if (beer.ratingsWeightedAverage) sets.push("beer.ratebeerWeightedAverage = {rb}.ratingsWeightedAverage");
    if (beer.desc) sets.push("beer.desc = {rb}.desc");
    if (beer.ibu) sets.push("beer.ibu = {rb}.ibu");
    if (beer.style) { 
      query += 'MERGE (style:style { name: {rb}.style }) ';
      query += 'MERGE beer-[:has_style]->style'
    }
    if (sets.length > 0) {
      query += 'SET ' + sets.join(', ');
      secondary.query(query, {product:product, rb: beer});
    }
    fetchRatebeerMetadata();
  }
}).on('end', function() {
  console.log('finished with ' + productCount + ' beers. committing transaction...');
  txn.commit(function(e, res) {
    if (e) return console.log(e);
    console.log('done');
    console.log('waiting for secondary data acquisition to finish...');
    var jcount = 0;
    var jobs = ops.map(function(op) { return function(cb) {op.then(function() { if (++jcount % 100 == 0) console.log('jcount =', jcount,'/',ops.length); cb() })}})
    async.parallel(jobs, function() {
      console.log('done');
      console.log('running ' + secondary.operations.length +  ' secondary additions...');
      secondary.commit(function(e,res ){
        if (e) return console.log(e);
        console.log('done');
      })
    });

  });
});



