/*
 * @Author: Sidharth Mishra
 * @Date:   2017-05-03 21:54:40
 * @Last Modified by:   Sidharth Mishra
 * @Last Modified time: 2017-05-04 11:20:54
 */

'use strict';

// my db
use archives_2000


// queries for trial stuff

// #4. Find articles by reporter name.

db.month_4.find({
    $and: [{
        "byline.person": {
            $elemMatch: {
                "firstname": "Constance",
                "middlename": "L.",
                "lastname": "HAYS",
                "role": "reported"
            }
        }
    }, {
        "document_type": "article"
    }]
})


//#5 -- Organization
db.month_4.find({
    "keywords": {
        $elemMatch: {
            "name": "organizations",
            "value": {
                $regex: /.*MATTEL.*/i
            }
        }
    }
})

//#5 -- People
db.month_4.find({
    "keywords": {
        $elemMatch: {
            "name": "persons",
            "value": {
                $regex: /.*CONDOLEEZZA.*/i
            }
        }
    }
})


// # 1
// for year 2005-2007 -- top 10 keywords a.k.a tags
db.month_4.aggregate([{
    $match: {
        "pub_date": {
            $regex: /200[5-7].*/i
        }
    }
}, {
    $unwind: "$keywords"
}, {
    $group: {
        _id: "$keywords",
        count: {
            $sum: 1
        }
    }
}, {
    $sort: {
        count: -1
    }
}, {
    $limit: 10
}])

// for year 2015 - 2017 -- top 10 keywords a.k.a tags
db.month_4.aggregate([{
    $match: {
        "pub_date": {
            $regex: /201[5-7].*/i
        }
    }
}, {
    $unwind: "$keywords"
}, {
    $group: {
        _id: "$keywords",
        count: {
            $sum: 1
        }
    }
}, {
    $sort: {
        count: -1
    }
}, {
    $limit: 10
}])
