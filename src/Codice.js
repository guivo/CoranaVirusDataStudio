/** Authorization not required */
function getAuthType() {
  var response = { type: 'NONE' };
  return response;
}

/** Perform the authorization */
function isAdminUser() {
  return true;
}

/** Provide a configuration  */
function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('The connector collects data from Johns Hopkins CSSE repository to include the outbreak data in a dashboard.');
  
  config.setDateRangeRequired(true);
  
  return config.build();
}

/** Return the expected fields */
function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
 
  fields.newDimension()
    .setId('Day')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('Country')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('Province')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('Lat')
    .setType(types.NUMBER);
  
  fields.newDimension()
    .setId('Long')
    .setType(types.NUMBER);
  
  fields.newMetric()
    .setId('Confirmed')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('ConfirmedDaily')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);;
  
  fields.newMetric()
    .setId('Deaths')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('DeathsDaily')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('Recovered')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  fields.newMetric()
    .setId('RecoveredDaily')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  return fields;
}

/** Return the schema of the data */
function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

/** Scan the text document from the repository to extract the data from the CSV
table, a record with the dates of the measurements and the row containing the data
from every monitored area. */
function formatContent(content) {
  // split by line the whole document
  var lines = content.split('\n');
  
  /* extract the day labels from the first row */
  var days = lines[0].split(',').slice(4).map(function(dayString) {    
    var dayParts = dayString.split('/');
    
    var d = new Date(2000+parseInt(dayParts[2]), parseInt(dayParts[0])-1, parseInt(dayParts[1]), 12);
    
    return d;
  });
  
  var rows = [];
  
  lines.slice(1).map(function(text) { // loop over the areas, one each line
    //var elems = text.split(',');
    var elems = text.match(/(\"[\w\s\d-.,()]+\",|[\w\s\d-.()]+,?|,)/g);
    
    if (elems==null) return; // an empty element found after the last
    
    // clean Province field
    elems = elems.map(function(el) {
      return el.replace(/[",]/g,'');
    });
    
    // fix Country names for DataStudio maps
    elems[1] = elems[1].replace("Mainland China","China");
    elems[1] = elems[1].replace("UK","Great Britain");
    
    return rows.push(elems);
  });
  
  return {
    days: days,
    rows: rows
  }
}

/** Convert the received data into a set of rows */
function responseToRows(requestedFields, content, dataset, dateRange) { 
  var endDate = new Date("1970-01-01");
  if ("endDate" in dateRange) {
    endDate = new Date(dateRange.endDate);
  }
  var startDate = new Date("2030-01-01");                         
  if ("startDate" in dateRange) {
    startDate = new Date(dateRange.startDate);
  }
  
  
  var hasDay = false;
  var rate = false;
  
  var idVal = -1;
  var idValDaily = -1;
  
  requestedFields.asArray().forEach(function(f, idField) {
    switch (f.getId()) {
      case "Day":      
        hasDay = true;
        return;
      case 'Confirmed':
      case 'Recovered':
      case 'Deaths':
        idVal = idField;
        return;
      case 'ConfirmedDaily':
      case 'RecoveredDaily':
      case 'DeathsDaily':
        idValDaily = idField;
        return;
    }
  });
  
  /* prepare to extract the day field respecting the given range */
  var skip = 0; // days to skip
  var nDays = 0; // total number of days in the range
  
  /* extract the labels from the first row, also count column to be skipped */
  var days = content.days.map(function(elem) {
    var d = new Date(elem);
    
    if (d<startDate) {
      skip += 1;
    }
    else {
      if (d<=endDate) nDays += 1;
    }
    
    var s = d.toISOString().split('T')[0].replace(/-/g, '');
    
    return s;
  });
  
  /* final clean to the days labels */
  days = days.slice(skip);
  
  var rows = [];
  
  content.rows.map(function(elems) { // loop over the areas    
    var common = elems.slice(0,4);
    
    var firstElem = 4+skip;
    var endElem = 4+skip+nDays+1;
    
    var prevTot = 0;
    var rowsArea = []
    
    elems.slice(firstElem,endElem).forEach(function (val, valIdx) { // loop over the days
      var row = []
      
      requestedFields.asArray().forEach(function (field, idField) { // loop over the fields
        var fieldId = field.getId();
        switch (fieldId) {
          case 'Country':
            return row.push(common[1]);
          case 'Province':
            return row.push(common[0]);
          case 'Lat':
            return row.push(common[2]);
          case 'Long':
            return row.push(common[3]);
          case 'Day':            
            return row.push(days[valIdx]);
          case 'Confirmed':
          case 'Deaths':
          case 'Recovered':
            return row.push(parseInt(val));
          case 'ConfirmedDaily':
          case 'DeathsDaily':
          case 'RecoveredDaily':
            return row.push(parseInt(val));
          default:
            return row.push('');
        }
      }); // end loop over the fields      
      
      return rowsArea.push({values: row});
    }); // end loop over the days
    
    /* evalute the rate day-by-day */
    if (idValDaily!=-1) {
      var prevVal = rowsArea[0].values[idValDaily];
      
      rowsArea = rowsArea.slice(1).map(function(v) {
        val = v.values[idValDaily]
        v.values[idValDaily] -= prevVal;
        prevVal = val;
        return v;
      });
    }
    
    if (!hasDay) {
      rowsArea = rowsArea.slice(rowsArea.length-1, rowsArea.length);
    }
    
    rows = rows.concat(rowsArea);
  }); // end loop over the areas
  
  return rows
}

/* combine datasets created from different sources */
function mixRows(datasetRows, requestedFields) {
  var rows = [];
  
  var idConfirmed = -1;
  var idConfirmedDaily = -1;
  var idDeaths = -1;
  var idDeathsDaily = -1;
  var idRecovered = -1;
  var idRecoveredDaily = -1;
  
  requestedFields.asArray().forEach(function (field, idx) { // loop over the fields        
    switch (field.getId()) {
      case 'Confirmed':
        idConfirmed = idx;
        return;
      case 'ConfirmedDaily':
        idConfirmedDaily = idx;
        return;
      case 'Deaths':
        idDeaths = idx;
        return;
      case 'DeathsDaily':
        idDeathsDaily = idx;
        return;
      case 'Recovered':
        idRecovered = idx;
        return;
      case 'RecoveredDaily':
        idRecoveredDaily = idx;
        return;
    }
  }); // end loop over the fields
  
  rows = datasetRows[0].rows;
  
  datasetRows.slice(1).forEach(function(ds) {
    var idVar = -1;
    switch(ds.variable) {
      case 'Confirmed':
        idVar = idConfirmed;
        break;
      case 'ConfirmedDaily':
        idVar = idConfirmedDaily;
        break;
      case 'Deaths':
        idVar = idDeaths;
        break;
      case 'DeathsDaily':
        idVar = idDeathsDaily;
        break;
      case 'Recovered':
        idVar = idRecovered;
        break;
      case 'RecoveredDaily':
        idVar = idRecoveredDaily;
        break;
    }
    
    rows.forEach(function(v,idx) {
      v.values[idVar] = ds.rows[idx].values[idVar];
    });
  });
  
  return rows;
}

/** Collect a document from an URL or the cache.

The cached document is pre-formatted to simplify the
next steps. */
function getCachedData(key, url) {
  var cache  = CacheService.getScriptCache();
  
  var cached = cache.get(key);
  if (cached!=null) {
    return JSON.parse(cached);
  }
  
  var response = UrlFetchApp.fetch(url);
  var content = response.getContentText();
  var formattedContent = formatContent(content);
  cache.put(key, JSON.stringify(formattedContent), 15000);
  return formattedContent;
}

/** Data request */
function getData(request) {
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);
  
  var dateRange = request.dateRange;
  var rate = 'configParams' in request ? 'rate' in request.configParams : false;

  var hasConfirmed = false;
  var hasRecovered = false;
  var hasDeaths = false;
  requestedFields.asArray().forEach(function (field) { // loop over the fields        
    switch (field.getId()) {
      case 'Confirmed':
      case 'ConfirmedDaily':
        hasConfirmed = true;
        return;
      case 'Deaths':
      case 'DeathsDaily':
        hasDeaths = true;
        return;
      case 'Recovered':
      case 'RecoveredDaily':
        hasRecovered = true;
        return;          
    }
  }); // end loop over the fields
  
  var responses = [];
  
  
  
  if (hasConfirmed || (!hasConfirmed&&!hasDeaths&&!hasRecovered)) {
    //var responseConfirmed = UrlFetchApp.fetch('https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv');
    var responseConfirmed = getCachedData("Confirmed", 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv');
    responses.push({variable: "Confirmed", rows: responseToRows(requestedFields, responseConfirmed, "Confirmed", dateRange)});
  }
 
  if (hasDeaths) {
    var responseDeaths = getCachedData("Deaths", 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv');
    responses.push({variable: "Deaths", rows: responseToRows(requestedFields, responseDeaths, "Deaths", dateRange)});
  }
  
  if (hasRecovered) {
    var responseRecovered = getCachedData("Recovered", 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv');
    responses.push({variable: "Recovered", rows: responseToRows(requestedFields, responseRecovered, "Recovered", dateRange)});
  }

  var rows = mixRows(responses, requestedFields);
  
  return {
    schema: requestedFields.build(),
    rows: rows
  };
}

function testSyntax() {
  var data = getData({
    fields: [{name: "Country"}, {name: "Province"}, {name: "Confirmed"}, {name: "Deaths"}, {name: "Recovered"}, {name: "Long"}],
    configParams: {
      rate: true
    },
    dateRange: {
      startDate: "2020-03-05",
      endDate: "2020-03-08"
    }
  });
 
  data;
}

function clearCache() {
  var cache  = CacheService.getScriptCache();
  cache.remove("Confirmed");
  cache.remove("Deaths");
  cache.remove("Recovered");
}
