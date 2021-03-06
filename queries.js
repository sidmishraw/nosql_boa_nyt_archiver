/*
 * @Author: Sidharth Mishra
 * @Date:   2017-05-03 21:54:40
 * @Last Modified by:   Sidharth Mishra
 * @Last Modified time: 2017-05-06 17:53:53
 */

'use strict';

// my db
use archives_2000


// queries for trial stuff

// #3. Search for articles based on user entry.
db.archives.find({
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

db.archives.find({
    $and: [{
        'byline.person.firstname': 'Constance'
    }, {
        'byline.person.middlename': 'L.'
    }, {
        'byline.person.lastname': 'HAYS'
    }, {
        'byline.person.role': 'reported'
    }, {
        'document_type': 'article'
    }]
})

db.archives.find({
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


// #5. Find articles about specific people or organizations. For eg - 
// search forarticles about Leonardo Dicaprio etc.
//#5 -- Organization
db.archives.find({
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
db.archives.find({
    "keywords": {
        $elemMatch: {
            "name": "persons",
            "value": {
                $regex: /.*CONDOLEEZZA.*/i
            }
        }
    }
})


//#2. Find the most popular news keywords from the entire archives collection.
db.archives.aggregate([{
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



// # 1. Compare the top news keywords for the years 2015-2017 and 2005-2007 tosee what the news 
// has been about. (Basically try and find the difference thathas come about in last 10 years.)
// for year 2005-2007 -- top 10 keywords a.k.a tags
db.archives.aggregate([{
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
db.archives.aggregate([{
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


// #6. Find the articles that have occured on page# x over these years.
db.archives.find({
    $and: [{
        "print_page": "90"
    }, {
        "document_type": "article"
    }]
})


// #8. Find the longest article (page or word count)
db.archives.find({
    "document_type": "article"
}).sort({
    "word_count": -1
}).limit(1)



// #10. Find the articles published in certain time range (date)
db.archives.find({
    $and: [{
        "pub_date": { $gt: "2000-04-01" }
    }, {
        "pub_date": { $lt: "2000-05-01" }
    }]
})

// #11. Find the organization that appears the most in NYT (organization)
db.archives.aggregate([{
    $unwind: '$keywords'
}, {
    $match: {
        $and: [{
            'keywords.name': "organizations"
        }, {
            'keywords.value': {
                $regex: /.+/
            }
        }]
    }
}, {
    $group: {
        '_id': '$keywords.value',
        'organization_count': {
            $sum: 1
        }
    }
}, {
    $sort: {
        'organization_count': -1
    }
}])


//# 12. Find the section-name for which maximum number of articles written
db.archives.aggregate([{
    $match: {
        "document_type": "article"
    }
}, {
    $group: {
        '_id': '$section_name',
        'section_count': {
            $sum: 1
        }
    }
}, {
    $sort: {
        'section_count': -1
    }
}])


//#9. Find the number of original article from NYT (source)
db.archives.aggregate([{
    $match: {
        "source": "The New York Times"
    }
}, {
    $group: {
        _id: "$source",
        orig_count: {
            $sum: 1
        }
    }
}])

//#14. Find which month had highest number of articles written
db.archives.aggregate([{
    $match: {
        "document_type": "article"
    }
}, {
    $group: {
        _id: "$pub_date",
        pub_count: {
            $sum: 1
        }
    }
}, {
    $sort: {
        pub_count: -1
    }
}])

// #15. Find 10 most popular article in the given timeframe
db.archives.find({
    $and: [{
        "pub_date": {
            $gt: "2000-04-01"
        }
    }, {
        "pub_date": {
            $lt: "2000-04-10"
        }
    }, {
        "print_page": "1"
    }, {
        "document_type": "article"
    }]
}).limit(10)
