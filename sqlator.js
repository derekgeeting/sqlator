var _ = require('lodash');
var async = require('async');
var chance = require('chance')();
var pg = require('pg');
var util = require('util');

var q = "with minMaxEventTimeIds as ("+
"            select"+
"                min(id),"+
"                max(id)"+
"            from dim_event_time det"+
"            where"+
"                det.day_as_timestamp >= '2014-10-01'"+
"        ),"+
"        periods as ("+
"            select"+
"                min(day_as_timestamp) as first_day_of_period,"+
"                array_agg(id) as ids,"+
"                week as period"+
"            from dim_event_time det"+
"            join minMaxEventTimeIds minMax on minMax.min <= det.id and minMax.max >= det.id"+
"            group by year, month, week"+
"            order by year asc, month asc, week asc"+
"        ),"+
"        event_stats as ("+
"          select"+
"            jt.first_name || ' ' || jt.last_name as name,"+
"             jt.last_name, jt.first_name, jt.email, jt.user_id, "+
"            stats.user_id as id,"+
"            coalesce(sum(case when stats.event_time_id = ANY (p.ids) then stats.count else 0 end), 0) as count,"+
"            p.first_day_of_period,"+
"            p.period"+
"          from periods p, users jt"+
"          join user_event_stats stats on stats.user_id = jt.id"+
"               and stats.event_time_id >= (select min from minMaxEventTimeIds) and stats.event_time_id <= (select max from minMaxEventTimeIds)"+
"          group by stats.user_id, jt.first_name, jt.last_name, jt.user_id, jt.email, p.first_day_of_period, p.period"+
"          order by stats.user_id, p.first_day_of_period asc"+
"        )"+
"        select"+
"          name,"+
"          id,"+
"           last_name, first_name, email, user_id, "+
"          sum("+
"            case"+
"              when period = 41 then count"+
"              else 0"+
"            end"+
"          ) as sort_count,"+
"          sum(count) as total_count"+
"        from event_stats"+
"          group by  last_name, first_name, email, user_id,  id, name"+
"        order by sort_count desc, total_count desc";

var db;
var dbDone;
var dbConnect = function(cb) {
	pg.connect('postgres://postgres@localhost/onesource_stats_1', function(err, client, done) {
		db = client;
		dbDone = done;
		cb(null);
	})
}

var doSql = function(cb) {
	db.query(q, function(err, result) {
		if(err) {
			console.log(err);
		}
		var rows = [];
		_.each(result.rows, function(r) {
			rows.push(r);
			/*
			var d = JSON.parse(r.data);
			var hasAddress3 = false;
			var hasReg = false;
			var hasTra = false;
			_.each(d.addresses, function(a) {
				hasAddress3 = hasAddress3 || a.address3;
				hasReg = hasReg || a.addressTypeCode==='REG';
				hasTra = hasTra || a.addressTypeCode==='TRA';
			})
			if(!hasReg && !hasTra && d.addresses.length>1) {
				rows.push({
					companyName: d.companyName,
					addresses: _.map(d.addresses, function(a) {
						return a.addressType+'   '+a.addressTypeCode+'   '+a.address1+a.address2+a.address3;
					})
				});
			}*/
		});
		cb(null, rows);
	});
}

async.series([
	dbConnect,
	doSql
], function(err, results) {
	if(err) {
		console.log('err',err);
	} else {
		console.log(JSON.stringify(results[1],null,'  '), results[1].length);
	}
});