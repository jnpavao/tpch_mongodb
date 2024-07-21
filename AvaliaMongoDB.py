from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from bson.regex import Regex

class AvaliaMongoDB():
    def __init__(self):

        # Connect to MongoDB
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['tpch-orders']  
        self.orders = self.db['orders']

        self.query1 = [{ "$unwind": "$lineitems" },
                            { "$match": { "lineitems.lShipdate": { "$lte": datetime(1998, 12, 1) } } },
                            {
                                "$group": {
                                    "_id": {
                                        "lReturnflag": "$lineitems.lReturnflag",
                                        "lLinestatus": "$lineitems.lLinestatus"
                                    },
                                    "sum_qty": { "$sum": "$lineitems.lQuantity" },
                                    "sum_base_price": { "$sum": "$lineitems.lExtendedprice" },
                                    "sum_disc_price": {
                                        "$sum": {
                                            "$multiply": [
                                                "$lineitems.lExtendedprice",
                                                { "$subtract": [1, "$lineitems.lDiscount"] }
                                            ]
                                        }
                                    },
                                    "sum_charge": {
                                        "$sum": {
                                            "$multiply": [
                                                "$lineitems.lExtendedprice",
                                                { "$subtract": [1, "$lineitems.lDiscount"] },
                                                { "$add": [1, "$lineitems.lTax"] }
                                            ]
                                        }
                                    },
                                    "avg_qty": { "$avg": "$lineitems.lQuantity" },
                                    "avg_price": { "$avg": "$lineitems.lExtendedprice" },
                                    "avg_disc": { "$avg": "$lineitems.lDiscount" },
                                    "count_order": { "$sum": 1 }
                                }
                            },
                            { "$sort": { "_id.lReturnflag": 1, "_id.lLinestatus": 1 } }
                        ]

        self.query3 = [
            {
                '$match': {
                    'customer.cMktsegment': 'BUILDING',
                    'oOrderdate': {'$lt': datetime(1995, 3, 15)},
                    'lineitems.lShipdate': {'$gt': datetime(1995, 3, 15)}
                }
            },
            {
                '$unwind': '$lineitems'
            },
            {
                '$group': {
                    '_id': {
                        'lOrderkey': '$lineitems.lOrderkey',
                        'o_orderdate': '$oOrderdate',
                        'o_shippriority': '$oShippriority'
                    },
                    'revenue': {
                        '$sum': {
                            '$multiply': ['$lineitems.lExtendedprice', {'$subtract': [1, '$lineitems.lDiscount']}]
                        }
                    }
                }
            },
            {
                '$sort': {'revenue': -1, '_id.o_orderdate': 1}
            }
        ]

        self.query4 = [
                {
                    '$match': {
                        'oOrderdate': {
                            '$gte': datetime(1995, 3, 15, 0,0,0),
                            '$lt': datetime(1995, 6, 15,0,0,0),
                        },
                        "$expr": {
                            "$lt": ["$lineitems.lCommitdate", "$lineitems.lReceiptdate"]
                        },
                    },
                },
                {
                    '$group': {
                        '_id': '$oOrderpriority',
                        'order_count': { '$sum': 1 },
                    },
                },
                {
                    '$sort': { '_id': 1 },
                },
        ]

        self.query5 = [
                {
                    "$unwind": "$lineitems"
                },
                {
                    '$match': {
                        'lineitems.partsupp.supplier.nation.region.rName': 'AMERICA',
                        'oOrderdate': {
                            '$gte': datetime(1995, 3, 15),
                            '$lt': datetime(1996, 3, 15)
                        },
                        '$expr': {
                                    '$eq': [
                                        '$lineitems.partsupp.supplier.nation.nNationkey',
                                        '$customer.nation.nNationkey'
                                    ]
                                }
                        
                    }
                },
                
                {
                    '$group': {
                        '_id': '$lineitems.partsupp.supplier.nation.nName',
                        'revenue': {
                            '$sum': {
                                '$multiply': ['$lineitems.lExtendedprice', {'$subtract': [1, '$lineitems.lDiscount']}]
                            }
                        }
                    }
                },
                {
                    '$sort': {'revenue': -1}
                }
            ]

        self.query6 = [
                    {
                        '$unwind': '$lineitems'
                    },
                    {
                        '$match': {
                            'lineitems.lShipdate': {
                                '$gte': datetime(1994, 1, 1),
                                '$lt': datetime(1995, 1, 1)
                            },
                            'lineitems.lDiscount': {'$gte': -0.01, '$lte': 0.01},
                            'lineitems.lQuantity': {'$lt': 24}
                        }
                    },
                    {
                        '$project': {
                            'revenue': {
                                '$multiply': [
                                    {
                                        '$cond': {
                                            'if': {'$eq': [{'$type': '$lineitems.lExtendedprice'}, 'array']},
                                            'then': {'$arrayElemAt': ['$lineitems.lExtendedprice', 0]},
                                            'else': '$lineitems.lExtendedprice'
                                        }
                                    },
                                    {
                                        '$cond': {
                                            'if': {'$eq': [{'$type': '$lineitems.lDiscount'}, 'array']},
                                            'then': {'$arrayElemAt': ['$lineitems.lDiscount', 0]},
                                            'else': '$lineitems.lDiscount'
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        '$group': {
                            '_id': None,
                            'revenue': {'$sum': '$revenue'}
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'revenue': 1
                        }
                    }
                ]
        
        self.query7 = [
                    {
                        '$unwind': "$lineitems"
                    },
                    {
                        '$match': {
                            '$or': [
                                {
                                    'lineitems.partsupp.supplier.nation.nName': 'FRANCE',
                                    'customer.nation.nName': 'GERMANY'
                                },
                                {
                                    'lineitems.partsupp.supplier.nation.nName': 'GERMANY',
                                    'customer.nation.nName': 'FRANCE'
                                }
                            ],
                            'lineitems.lShipdate': {
                                '$gte': datetime(1995, 1, 1),
                                '$lte': datetime(1996, 12, 31)
                            }
                        }
                    },
                    {
                        '$group': {
                            '_id': {
                                'supp_nation': '$lineitems.partsupp.supplier.nation.nName',
                                'cust_nation': '$customer.nation.nName',
                                'l_year': {'$year': '$lineitems.lShipdate'}
                            },
                            'revenue': {
                                '$sum': {
                                    '$multiply': ['$lineitems.lExtendedprice', {'$subtract': [1, '$lineitems.lDiscount']}]
                                }
                            }
                        }
                    },
                    {
                        '$sort': {
                        '_id.supp_nation': 1,
                            '_id.cust_nation': 1,
                            '_id.l_year': 1
                        }
                    }
                ]

        self.query8 = [
                {"$unwind": "$lineitems"},
                {
                    "$match": {
                        "oOrderdate": {
                            "$gte": datetime(1995, 1, 1),
                            "$lte": datetime(1996, 12, 31)
                        },
                        "lineitems.partsupp.supplier.nation.region.rName": "AMERICA",
                        "lineitems.partsupp.part.pType": "ECONOMY ANODIZED STEEL"
                    }
                },
                {
                    "$group": {
                        "_id": {"$year": "$oOrderdate"},
                        "volume": {
                            "$sum": {
                                "$multiply": ["$lineitems.lExtendedprice", {"$subtract": [1, "$lineitems.lDiscount"]}]
                            }
                        },
                        "nation": {"$first": "$lineitems.partsupp.supplier.nation.nName"}
                    }
                },
                {
                    "$group": {
                        "_id": "$_id",
                        "total_volume": {"$sum": "$volume"},
                        "us_volume": {
                            "$sum": {
                                "$cond": [
                                    {"$eq": ["$nation", "UNITED STATES"]},
                                    "$volume",
                                    0
                                ]
                            }
                        }
                    }
                },
                {
                    "$addFields": {
                        "mkt_share": {"$divide": ["$us_volume", "$total_volume"]}
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "mkt_share": 1
                    }
                },       
                {"$sort": {"_id": 1}}
            ]

        self.query9 = [
                    {
                        "$match": {
                            "lineitems.partsupp.part.pName": { "$regex": ".*green.*" }
                        }
                    },
                    {
                        "$project": {
                            "nation": "$lineitems.partsupp.supplier.nation.nName",
                            "o_year": { "$year": "$oOrderdate" },
                            "amount": {
                                    "$map":{
                                        "input":{"$range":[0,{"$size":"$lineitems"}]},
                                        "as":"ix",
                                        "in":{
                                            "$let":{
                                                "vars":{
                                                    "line":{"$arrayElemAt":["$lineitems","$$ix"]}
                                                },
                                                "in":{
                                                    "$subtract": [
                                                        { "$multiply": ["$$line.lExtendedprice", { "$subtract": [1, "$$line.lDiscount"] }] },
                                                        { "$multiply": ["$$line.partsupp.psSupplycost", "$$line.lQuantity"] }
                                                    ]
                                                }
                                            }
                                        }
                                    }
                            }
                        }
                    },
                    {
                        "$project": {
                            "sum_profit":{
                                "$map": {
                                    "input": {"$range": [0, {"$size": "$amount"}]},
                                    "as": "ix",
                                    "in": {
                                        "$let": {
                                            "vars": {
                                                "line": {"$arrayElemAt": ["$amount", "$$ix"]}
                                            },
                                            "in": {
                                                "$sum": [ "$$line" ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"nation": "$nation", "o_year": "$o_year", "sumprofit" :"$sum_profit"},
                        }
                    },
                    {
                        "$sort": {"_id.nation": 1, "_id.o_year": -1}
                    }
            ]

        self.query10 = [
                            {
                                '$unwind': '$lineitems'
                            },
                            {
                                '$match': {
                                    'oOrderdate': {'$gte': datetime(1993,10,1), '$lt': datetime(1994,1,1)},
                                    'lineitems.lReturnflag': 'R'
                                }
                            },
                            {
                                '$group': {
                                    '_id': {
                                        'cCustkey': '$customer.cCustkey',
                                        'cName': '$customer.cName',
                                        'cAcctbal': '$customer.cAcctbal',
                                        'cPhone': '$customer.cPhone',
                                        'nName': '$customer.nation.nName',
                                        'cAddress': '$customer.cAddress',
                                        'cComment': '$customer.cComment'
                                    },
                                    'revenue': {
                                        '$sum': {
                                            '$multiply': ['$lineitems.lExtendedprice', {'$subtract': [1, '$lineitems.lDiscount']}]
                                        }
                                    }
                                }
                            },
                            {
                                '$sort': {'revenue': -1}
                            }
                        ]
        
        self.query12 = [
                        {"$unwind": "$lineitems"},
                        {"$match": {
                            "lineitems.lShipmode": {"$in": ["MAIL", "SHIP"]},
                            "lineitems.lReceiptdate": {
                                    "$gte": datetime(1994,1,1),
                                    "$lt": datetime(1995,1,1)
                                },
                            
                            "$expr": {
                                "$lt": ["$lineitems.lCommitdate", "$lineitems.lReceiptdate"]
                            },
                            "$expr": {
                                "$lt": ["$lineitems.lShipdate", "$lineitems.lCommitdate"]
                            }
                        }},
                        {"$group": {
                            "_id": "$lineitems.lShipmode",
                            "high_line_count": {
                                "$sum": {
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$oOrderpriority", "1-URGENT"]},
                                            {"$eq": ["$oOrderpriority", "2-HIGH"]}
                                        ]},
                                        1,
                                        0
                                    ]
                                }
                            },
                            "low_line_count": {
                                "$sum": {
                                    "$cond": [
                                        {"$and": [
                                            {"$ne": ["$oOrderpriority", "1-URGENT"]},
                                            {"$ne": ["$oOrderpriority", "2-HIGH"]}
                                        ]},
                                        1,
                                        0
                                    ]
                                }
                            }
                        }},
                        {"$sort": {"_id": 1}}
                    ]

        self.query13 = [
                        {
                            "$match": {
                                "$expr": {
                                    "$not": { "$regexMatch": { "input": "$oComment", "regex": "%special%requests%" } }
                                }
                            }
                        },
                        {
                            "$group": {
                                "_id": "$customer.cCustkey",
                                "c_count": { "$sum": 1 }
                            }
                        },
                        {
                            "$group": {
                                "_id": "$c_count",
                                "custdist": { "$sum": 1 }
                            }
                        },
                        {
                            "$sort": { "custdist": -1, "_id": -1 }
                        }
                    ]

        self.query14 = [
                            {"$unwind": "$lineitems"},
                            {"$match": {
                                "lineitems.lShipdate": {
                                    "$gte": datetime(1995,9,1),
                                    "$lt": datetime(1995,10,1)
                                }
                            }},
                            {"$group": {
                                "_id": None,
                                "total": {
                                    "$sum": {
                                        "$multiply": ["$lineitems.lExtendedprice", {"$subtract": [1, "$lineitems.lDiscount"]}]
                                    }
                                },
                                "promoTotal": {
                                    "$sum": {
                                        "$cond": [
                                            {"$eq": [{"$substrCP": ["$lineitems.partsupp.part.pType", 0, 5]}, "PROMO"]},
                                            {"$multiply": ["$lineitems.lExtendedprice", {"$subtract": [1, "$lineitems.lDiscount"]}]},
                                            0
                                        ]
                                    }
                                }
                            }},
                            {"$project": {
                                "_id": 0,
                                "promo_revenue": {
                                    "$multiply": [
                                        100,
                                        {"$divide": ["$promoTotal", "$total"]}
                                    ]
                                }
                            }}
                        ]

        self.query18 = [
                        {
                            "$unwind": "$lineitems"
                        },
                        {
                            "$group": {
                                "_id": {
                                    "cName": "$customer.cName",
                                    "cCustkey": "$customer.cCustkey",
                                    "oOrderkey": "$oOrderkey",
                                    "oOrderdate": "$oOrderdate",
                                    "oTotalprice": "$oTotalprice"
                                },
                                "totalQuantity": {"$sum": "$lineitems.lQuantity"}
                            }
                        },
                        {
                            "$match": {
                                "totalQuantity": {"$gt": 300}
                            }
                        },
                        {
                            "$sort": {
                                "_id.oTotalprice": -1,
                                "_id.oOrderdate": 1
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "cName": "$_id.cName",
                                "cCustkey": "$_id.cCustkey",
                                "oOrderkey": "$_id.oOrderkey",
                                "oOrderdate": "$_id.oOrderdate",
                                "oTotalprice": "$_id.oTotalprice",
                                "totalQuantity": 1
                            }
                        }
                    ]

        self.query19 = [
                    {
                        '$unwind': '$lineitems'
                    },
                    {
                        '$match': {
                            '$or': [
                                {
                                    'lineitems.partsupp.part.pBrand': 'Brand#12',
                                    'lineitems.partsupp.part.pContainer': { '$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'] },
                                    'lineitems.lQuantity': { '$gte': 1, '$lte': 11 },
                                    'lineitems.partsupp.part.pSize': { '$gte': 1, '$lte': 5 },
                                    'lineitems.lShipmode': { '$in': ['AIR', 'AIR REG'] },
                                    'lineitems.lShipinstruct': 'DELIVER IN PERSON'
                                },
                                {
                                    'lineitems.partsupp.part.pBrand': 'Brand#23',
                                    'lineitems.partsupp.part.pContainer': { '$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'] },
                                    'lineitems.lQuantity': { '$gte': 10, '$lte': 20 },
                                    'lineitems.partsupp.part.pSize': { '$gte': 1, '$lte': 10 },
                                    'lineitems.lShipmode': { '$in': ['AIR', 'AIR REG'] },
                                    'lineitems.lShipinstruct': 'DELIVER IN PERSON'
                                },
                                {
                                    'lineitems.partsupp.part.pBrand': 'Brand#34',
                                    'lineitems.partsupp.part.pContainer': { '$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'] },
                                    'lineitems.lQuantity': { '$gte': 20, '$lte': 30 },
                                    'lineitems.partsupp.part.pSize': { '$gte': 1, '$lte': 15 },
                                    'lineitems.lShipmode': { '$in': ['AIR', 'AIR REG'] },
                                    'lineitems.lShipinstruct': 'DELIVER IN PERSON'
                                }
                            ]
                        }
                    },
                    {
                        '$group': {
                            '_id': None,
                            'revenue': {
                                '$sum': {
                                    '$multiply': ['$lineitems.lExtendedprice', { '$subtract': [1, '$lineitems.lDiscount'] }]
                                }
                            }
                        }
                    }
                ]

        self.query20 = [
            # Step 1: Unwind the lineitems array to access individual line items
            {
                '$unwind': '$lineitems'
            },
            # Step 2: Unwind the partsupp array within lineitems
            {
                '$unwind': '$lineitems.partsupp'
            },
            # Step 3: Match partsupp documents where part name starts with 'forest%'
            {
                '$match': {
                    'lineitems.partsupp.part.pName': {'$regex': '^forest'}
                }
            },
            # Step 4: Filter lineitems within the specified date range and calculate total quantity
            {
                '$group': {
                    '_id': {
                        'psPartkey': '$lineitems.partsupp.psPartkey',
                        'psSuppkey': '$lineitems.partsupp.psSuppkey'
                    },
                    'totalQuantity': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$and': [
                                        {'$eq': ['$lineitems.partsupp.psPartkey', '$lineitems.partsupp.psPartkey']},
                                        {'$eq': ['$lineitems.partsupp.psSuppkey', '$lineitems.partsupp.psSuppkey']},
                                        {'$gte': ['$lineitems.lShipdate', datetime(1994, 1, 1)]},
                                        {'$lt': ['$lineitems.lShipdate', datetime(1995, 1, 1)]}
                                    ]
                                },
                                '$lineitems.lQuantity',
                                0
                            ]
                        }
                    },
                    'lineitem': {'$first': '$lineitems'}
                }
            },
            # Step 5: Filter partsupp based on the availability condition
            {
                '$match': {
                    '$expr': {
                        '$gt': ['$lineitem.partsupp.psAvailqty', {'$multiply': [0.5, '$totalQuantity']}]
                    }
                }
            },
            # Step 6: Match suppliers where nation name is 'CANADA'
            {
                '$match': {
                    'lineitem.partsupp.supplier.nation.nName': 'CANADA'
                }
            },
            # Step 7: Project the required fields
            {
                '$project': {
                    'sName': '$lineitem.partsupp.supplier.sName',
                    'sAddress': '$lineitem.partsupp.supplier.sAddress'
                }
            },
            # Step 8: Sort by supplier name
            {
                '$sort': {'sName': 1}
            }
        ]

        self.query21 = [
                # Match documents where nation name is 'SAUDI ARABIA'
                {
                    '$match': {
                        'customer.nation.nName': 'SAUDI ARABIA'
                    }
                },
                # Unwind the lineitems array
                {
                    '$unwind': '$lineitems'
                },
                # Match documents where order status is 'F' and receipt date is greater than commit date
                {
                    '$match': {
                        'oOrderstatus': 'F',
                        '$expr': {'$lt': [
                                    'lineitems.lReceiptdate',
                                    '$lineitems.lCommitdate'
                                    ] 
                                }
                    }
                },
                # Left outer join with lineitem collection to simulate EXISTS
                {
                    '$lookup': {
                        'from': 'lineitem',
                        'let': {'orderkey': '$lineitems.lOrderkey', 'suppkey': '$lineitems.lSuppkey'},
                        'pipeline': [
                            {'$match': {
                                '$expr': {
                                    '$and': [
                                        {'$eq': ['$lOrderkey', '$$orderkey']},
                                        {'$ne': ['$lSuppkey', '$$suppkey']}
                                    ]
                                }
                            }},
                            {'$limit': 1}  
                        ],
                        'as': 'exists_docs'
                    }
                },
                # Filter out documents where the condition is not met
                {
                    '$match': {
                        'exists_docs':  []
                    }
                },
                # Group by supplier name and count the number of documents
                {
                    '$group': {
                        '_id': '$lineitems.partsupp.supplier.sName',
                        'numwait': {'$sum': 1}
                    }
                },
                # Sort by numwait in descending order and then by supplier name
                {
                    '$sort': {'numwait': -1, '_id': 1}
                }
            ]



    def exec_query2(self):
        sub_query_pipeline = [
            {
                '$match': {
                    'lineitems.partsupp.supplier.nation.region.rName': 'EUROPE'
                }
            },
            {
                '$group': {
                    '_id': '',
                    'minSupplyCost': {'$min': '$lineitems.partsupp.psSupplycost'}
                }
            }
        ]

        # Execute the subquery to get the minimum supply cost
        ini_exec = datetime.now()
        sub_query_result = list(self.orders.aggregate(sub_query_pipeline))
        fim_exec = datetime.now()
        t1 = fim_exec - ini_exec

        min_supply_cost =  sub_query_result[0]['minSupplyCost'][0]
        
        main_query_pipeline = [ {
                        '$match': {
                            'lineitems.partsupp.part.pSize': 15,
                            'lineitems.partsupp.part.pType': {'$regex': 'BRASS'},
                            'lineitems.partsupp.supplier.nation.region.rName': 'EUROPE',
                            'lineitems.partsupp.psSupplycost': min_supply_cost
                        }
                    },
                    {
                        '$sort': {
                            'lineitems.partsupp.supplier.sAcctbal': -1,
                            'lineitems.partsupp.supplier.nation.nName': 1,
                            'lineitems.partsupp.supplier.sName': 1,
                            'lineitems.partsupp.part.pPartkey': 1
                        }
                    },
                    {
                        '$project': {
                            'sAcctbal': '$lineitems.partsupp.supplier.sAcctbal',
                            'sName': '$lineitems.partsupp.supplier.sName',
                            'nName': '$lineitems.partsupp.supplier.nation.nName',
                            'pPartkey': '$lineitems.partsupp.part.pPartkey',
                            'pMfgr': '$lineitems.partsupp.part.pMfgr',
                            'sAddress': '$lineitems.partsupp.supplier.sAddress',
                            'sPhone': '$lineitems.partsupp.supplier.sPhone',
                            'sComment': '$lineitems.partsupp.supplier.sComment'
                        }
                    }
                ]

        ini_exec = datetime.now()
        result = self.orders.aggregate(main_query_pipeline)
        fim_exec = datetime.now()
        t2 = fim_exec - ini_exec
        return t1 + t2

    def exec_query11(self):
        subQueryPipeline = [
                    {
                        "$unwind": "$lineitems"
                    },
                    {
                        "$match": {
                            "lineitems.partsupp.supplier.nation.nName": "GERMANY"
                        }
                    },
                    {
                        "$group": {
                            "_id": '',
                            "total": {
                                "$sum": {
                                    "$multiply": ["$lineitems.partsupp.psSupplycost", "$lineitems.partsupp.psAvailqty",0.0001]
                                }
                            }
                        }
                    }
                ]
        ini_exec = datetime.now()
        subQuery = list(self.orders.aggregate(subQueryPipeline))
        fim_exec = datetime.now()
        t1 = fim_exec - ini_exec

        # Main query
        mainQueryPipeline = [
                {
                    "$unwind": "$lineitems"
                },
                {
                    "$match": {
                        "lineitems.partsupp.supplier.nation.nName": "GERMANY"
                    }
                },
                {
                    "$group": {
                        "_id": "$lineitems.partsupp.psPartkey",
                        "value": {
                            "$sum": {
                                "$multiply": ["$lineitems.partsupp.psSupplycost", "$lineitems.partsupp.psAvailqty"]
                            }
                        }
                    }
                },
                {
                    "$match": {
                        "value": {
                            "$gt": subQuery[0]["total"]
                        }
                    }
                },
                {
                    "$sort": {
                        "value": -1
                    }
                }
            ]
        ini_exec = datetime.now()
        result = self.orders.aggregate(mainQueryPipeline)
        fim_exec = datetime.now()
        t2 = fim_exec - ini_exec
        return t1 + t2

    def exec_query15(self):
        create_view_pipeline = [
                {
                    "$unwind": "$lineitems"
                },
                {
                    "$match": {
                        "lineitems.lShipdate": {
                            "$gte": datetime(1996,1,1),
                            "$lt": datetime(1996,4,1)  
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$lineitems.partsupp.supplier.sSuppkey",
                        "total_revenue": {
                            "$sum": {
                                "$multiply": ["$lineitems.lExtendedprice", {"$subtract": [1, "$lineitems.lDiscount"]}]
                            }
                        }
                    }
                }    
            ]
        ini_exec = datetime.now()
        # Create the view
        self.db.command("create", "revenue0", viewOn="orders", pipeline=create_view_pipeline)
        fim_exec = datetime.now()
        t1 = fim_exec - ini_exec


        # Find maximum revenue
        max_revenue_pipeline = [
                {"$sort": {"total_revenue": -1}}, 
                {"$limit": 1}  
            ]
        ini_exec = datetime.now()
        max_revenue_result = list(self.db['revenue0'].aggregate(max_revenue_pipeline))
        fim_exec = datetime.now()
        t2 = fim_exec - ini_exec
        
        # Identify the supplier's id
        id_max_revenue = max_revenue_result[0]['_id'] if max_revenue_result else 0
        main_pipeline = [
                    {
                        "$unwind":'$lineitems'
                    },
                    {"$match": {'lineitems.partsupp.supplier.sSuppkey': id_max_revenue}},
                    
                    {"$project": {
                        'sSuppkey': '$lineitems.partsupp.supplier.sSuppkey',
                        'sName': '$lineitems.partsupp.supplier.sName',
                        'sAddress': '$lineitems.partsupp.supplier.sAddress',
                        'sPhone': '$lineitems.partsupp.supplier.sPhone',
                        'total_revenue': '$revenue_info.total_revenue'
                    }},
                    {"$sort": {'lineitems.partsupp.supplier.sSuppkey': -1}}
                    
                ]
        
        ini_exec = datetime.now()
        result = list(self.db['orders'].aggregate(main_pipeline))
        fim_exec = datetime.now()
        t3 = fim_exec - ini_exec
        return t1 + t2 + t3
        

    def exec_query16(self):

        ini_exec = datetime.now()
        orders_cursor = self.orders.find({'lineitems.partsupp.supplier.sComment': Regex('.*Customer.*Complaints.*')})
        # Extracting unique supplier keys from the cursor
        suppkeys = []
        for order in orders_cursor:
            for lineitem in order['lineitems']:
                for partsupp_key, partsupp_value in lineitem['partsupp'].items():
                    if partsupp_key == 'supplier': 
                        suppkeys.append(partsupp_value['sSuppkey'])
            
        fim_exec = datetime.now()
        t1 = fim_exec - ini_exec        

        # Define the aggregation pipeline
        pipeline = [
                {
                    '$unwind': '$lineitems'
                },
                {
                    '$unwind': '$lineitems.partsupp'
                },
                {
                    '$match': {
                        'lineitems.partsupp.part.pBrand': {'$ne': 'Brand#45'},
                        'lineitems.partsupp.part.pType': {'$not': Regex('^MEDIUM POLISHED.*','i')},
                        'lineitems.partsupp.part.pSize': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
                        'lineitems.partsupp.psSuppkey': {'$nin': suppkeys}
                    }
                },
                {
                    '$group': {
                        '_id': {'pBrand': '$lineitems.partsupp.part.pBrand', 'pType': '$lineitems.partsupp.part.pType', 'pSize': '$lineitems.partsupp.part.pSize'},
                        'supplier_cnt': {'$sum': 1}
                    }
                },
                {
                    '$sort': {
                        'supplier_cnt': -1,
                        '_id.pBrand': 1,
                        '_id.pType': 1,
                        '_id.pSize': 1
                    }
                }     

                    
            ]
        ini_exec = datetime.now()
        result = self.orders.aggregate(pipeline)
        fim_exec = datetime.now()
        t2 = fim_exec - ini_exec 
        return t1+t2
    
    def exec_query17(self):

        ini_exec = datetime.now()
        avg_quantity_cursor = list(self.orders.aggregate([
                                {"$unwind": "$lineitems"},
                                {"$group": {
                                    "_id": "$lineitems.lPartkey",
                                    "avgQuantity": {"$avg": "$lineitems.lQuantity"}
                                }}
                            ]))
    
        avg_quantity = avg_quantity_cursor[0]['avgQuantity']
        valor = 0.2 * float(avg_quantity)

        fim_exec = datetime.now()
        t1 = fim_exec - ini_exec 

        pipeline = [{"$unwind": "$lineitems"},
                    {"$match": {
                        "lineitems.partsupp.part.pBrand": "Brand#23",
                        "lineitems.partsupp.part.pContainer": "MED BOX",
                        "lineitems.lQuantity": {"$lt": valor}
                    }},
                    {"$group": {
                        "_id": None,
                        "total": {"$sum": "$lineitems.lExtendedprice"}
                    }},
                    {"$project": {
                        "avg_yearly": {"$divide": ["$total", 7.0]}
                    }}
                    ]

        ini_exec = datetime.now()
        result = self.orders.aggregate(pipeline)
        fim_exec = datetime.now()
        t2 = fim_exec - ini_exec
        return t1 + t2 
    
    def exec_query22(self):

        ini_exec = datetime.now()
        # Calculate average account balance
        avg_acctbal_cursor = self.orders.aggregate([
            {
                "$match": {
                    "customer.cAcctbal": { "$gt": 0.00 },
                    "$expr": {
                    "$in": [
                        { "$substr": ["$customer.cPhone", 0, 2] },
                        ['13', '31', '23', '29', '30', '18', '17']
                    ]
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avgAcctbal": { "$avg": "$customer.cAcctbal" }
                }
            }
            
        ])



        avg_acctbal_result = list(avg_acctbal_cursor)
        avg_acctbal = avg_acctbal_result[0]["avgAcctbal"] if avg_acctbal_result else 0.00
        fim_exec = datetime.now()
        t1 = fim_exec - ini_exec

        # Query based on average account balance
        main_query_pipeline = [
            {
                "$match": {
                    "customer.cAcctbal": { "$gt": avg_acctbal },
                    "$expr": {"$in": [
                                { "$substr": ["$customer.cPhone", 0, 2] },
                                ['13', '31', '23', '29', '30', '18', '17']
                            ]
                            },
                    "oCustkey": { "$exists": 'false' }
                    
                }
            },
            {
                "$group": {
                    "_id": { "$substr": ["$customer.cPhone", 0, 2] },
                    "numcust": { "$sum": 1 },
                    "totacctbal": { "$sum": "$customer.cAcctbal" }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "cntrycode": "$_id",
                    "numcust": 1,
                    "totacctbal": 1
                }
            },
            {
                "$sort": {
                    "cntrycode": 1
                }
            }
            
        ]
        # Execute the aggregation pipeline
        ini_exec = datetime.now()
        result = self.orders.aggregate(main_query_pipeline)
        fim_exec = datetime.now()
        t2 = fim_exec - ini_exec
        return t1+t2
        

    def executa_todas(self, qtd_vezes: int):

        df_tempos = pd.DataFrame()

        for j in range(1, qtd_vezes+1):
            # Executa as 22 queries
            for i in range(1, 23):
                print(f'Execução {j} - Consulta Q{i}...')

                # Gera o nome da variável dinamicamente
                nome_variavel = f'query{i}'

                if  i == 2:
                    tempo = self.exec_query2()
                elif i == 11:
                    tempo = self.exec_query11()
                elif i == 15:
                    tempo = self.exec_query15()
                elif i == 16:
                    tempo = self.exec_query16()
                elif i == 17:
                    tempo = self.exec_query17()
                elif i == 22:
                    tempo = self.exec_query22()
                else:
                    # Obtém o valor do atributo query e executa a consulta
                    query = getattr(self, nome_variavel)      

                    ini_exec = datetime.now()
                    result = self.orders.aggregate(query)
                    fim_exec = datetime.now()
                    tempo = fim_exec - ini_exec

                
                # Salvando o resultado
                df_tempos = pd.concat([df_tempos,pd.DataFrame([['MongoDB',j,nome_variavel,tempo.total_seconds()]])], ignore_index=True)

        df_tempos.columns = ['banco_de_dados', 'num_execucao', 'consulta', 'tempo']
        # Formata a data e hora para incluir no nome do arquivo
        data_hora = datetime.now().strftime("%Y-%m-%d_%Hh-%Mm-%Ss")
        nome_arquivo = f"resultados/resultado_mongodb_{data_hora}.xlsx"
        df_tempos.to_excel(nome_arquivo, index=False)
        print(f'Arquivo {nome_arquivo} gerado com sucesso.')

    def executa_consulta(self,query):
        
        result = list(self.orders.aggregate(query))
        print(result)
        
        

# Início da execução
if __name__ == "__main__":
    print("#### AvaliaMongoDB.py #### ")
    AvaliaMongoDB().executa_todas(10)
    #obj = AvaliaMongoDB()
    #obj.executa_consulta(obj.query8)
    #print(obj.exec_query15())
    
