/*
 * @Author: Sidharth Mishra
 * @Date:   2017-05-03 21:54:40
 * @Last Modified by:   Sidharth Mishra
 * @Last Modified time: 2017-05-05 11:43:53
 */

'use strict';

// my db
use archives_2000


// queries for trial stuff

// #3
db.month_4.find({
    $and: [{
        "document_type": "article"
    }, {

        $or: [{
            "lead_paragraph": {
                $regex: /.*of the need.*/
            }
        }, {
            "snippet": {
                $regex: /.*of the need.*/
            }
        }, {
            "abstract": {
                $regex: /.*of the need.*/
            }
        }]
    }]
})


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


//#2Find the most popular news keywords from the entire archives collection.
db.month_4.aggregate([{
    $match: {
        "type_of_material": "News"
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
    $limit: 5
}])



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


// Query#6. Find the articles that have occured on page# x over these years.
db.month_4.find({
    $and: [{
        "print_page": "90"
    }, {
        "document_type": "article"
    }]
})


//Query#8. Find the longest article (page or word count)
db.month_4.find({
    "document_type": "article"
}).sort({
    "word_count": -1
}).limit(1)



// #10
db.month_4.find({
    $and: [{
        "pub_date": { $gt: "2000-04-01" }
    }, {
        "pub_date": { $lt: "2000-05-01" }
    }]
})
