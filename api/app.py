from flask import Flask, jsonify, request
from flask_cors import CORS
import pymongo
from datetime import datetime

app = Flask(__name__)
CORS(app)

# MongoDB connection
def get_mongo_client():
    client = pymongo.MongoClient("mongodb://mongodb-external:27017/")
    return client

# API to get companies
@app.route('/api/companies', methods=['GET'])
def get_companies():
    client = get_mongo_client()
    db = client["Yfinance_Load"]
    collections = db.list_collection_names()
    client.close()
    return jsonify({"status": "success", "companies": collections})

# API to get company data with dynamic filtering based on agg_type and period_key
@app.route('/api/data/<company_name>', methods=['GET'])
def get_company_data(company_name):
    company_name = company_name.replace("_", " ")  # Normalisasi nama
    client = get_mongo_client()
    db = client["Yfinance_Load"]

    if company_name not in db.list_collection_names():
        client.close()
        return jsonify({"status": "error", "message": f"Company {company_name} not found"}), 404

    collection = db[company_name]

    # Ambil query parameter untuk filter
    agg_type = request.args.get('agg_type')  # day, month, year
    period_key = request.args.get('period_key')  # Misal: 2014-01-02, 2023-01, 2022
    start_period = request.args.get('start_period')
    end_period = request.args.get('end_period')

    # Bangun query untuk filter
    query = {}
    if agg_type:
        query['agg_type'] = agg_type  # Mengambil semua data dengan agg_type tertentu
    if period_key:
        query['period_key'] = period_key
    elif start_period and end_period:
        query['period_key'] = {"$gte": start_period, "$lte": end_period}

    # Ambil data dari MongoDB tanpa batasan limit
    data = list(collection.find(query))

    # Format data
    for item in data:
        item['_id'] = str(item['_id'])

        # Format 'Date' jika ada
        if 'Date' in item:
            try:
                if isinstance(item['Date'], datetime):
                    item['Date'] = item['Date'].strftime("%Y-%m-%dT%H:%M:%S%z")  # Formatkan dengan timezone
            except Exception as e:
                pass

    client.close()

    return jsonify({
        "status": "success",
        "company": company_name,
        "filters": {
            "agg_type": agg_type,
            "period_key": period_key,
            "start_period": start_period,
            "end_period": end_period
        },
        "count": len(data),
        "data": data
    })

# API to get available aggregation types for a company
@app.route('/api/agg_types/<company_name>', methods=['GET'])
def get_agg_types(company_name):
    client = get_mongo_client()
    db = client["Yfinance_Load"]

    if company_name not in db.list_collection_names():
        client.close()
        return jsonify({"status": "error", "message": f"Company {company_name} not found"}), 404

    collection = db[company_name]
    agg_types = collection.distinct("agg_type")
    client.close()

    return jsonify({
        "status": "success",
        "company": company_name,
        "agg_types": agg_types
    })

# API to get available period keys for a company with optional agg_type filter
@app.route('/api/period_keys/<company_name>', methods=['GET'])
def get_period_keys(company_name):
    client = get_mongo_client()
    db = client["Yfinance_Load"]

    if company_name not in db.list_collection_names():
        client.close()
        return jsonify({"status": "error", "message": f"Company {company_name} not found"}), 404

    collection = db[company_name]

    # Get query parameters
    agg_type = request.args.get('agg_type')  # Optional filter by agg_type

    # Build query filter
    query = {}
    if agg_type:
        query['agg_type'] = agg_type

    period_keys = collection.distinct("period_key", query)
    client.close()

    return jsonify({
        "status": "success",
        "company": company_name,
        "agg_type": agg_type if agg_type else "all",
        "period_keys": period_keys
    })

# ==================== IQPLUS ENDPOINTS ====================
# API semua news
@app.route('/api/iqplus/news', methods=['GET'])
def get_iqplus_news():
    client = get_mongo_client()
    db = client["Iqplus"]
    collection = db["Iqplus_News_Transform"]

    # Get query parameters for filtering
    title_search = request.args.get('search')  # Search in title
    
    # Build query
    query = {}
    if title_search:
        query['title'] = {"$regex": title_search, "$options": "i"}  # Case insensitive search

    # Get total count
    total_count = collection.count_documents(query)

    # Fetch ALL data (no limit/skip)
    cursor = collection.find(query).sort([("metadata.original_date", -1)])
    
    # Format data - only return requested fields
    formatted_data = []
    for item in cursor:
        formatted_item = {
            "_id": str(item["_id"]),
            "title": item.get("title", ""),
            "summary": item.get("summary", ""),
            "original_content": item.get("original_content", ""),
            "original_date": item.get("metadata", {}).get("original_date", "")
        }
        formatted_data.append(formatted_item)

    client.close()

    return jsonify({
        "status": "success",
        "total_count": total_count,
        "returned_count": len(formatted_data),
        "data": formatted_data
    })

# API news berdasarkan ID
@app.route('/api/iqplus/news/<news_id>', methods=['GET'])
def get_iqplus_news_by_id(news_id):
    from bson import ObjectId
    
    client = get_mongo_client()
    db = client["Iqplus"]
    collection = db["Iqplus_News_Transform"]

    try:
        # Find specific news by ObjectId
        item = collection.find_one({"_id": ObjectId(news_id)})
        
        if not item:
            client.close()
            return jsonify({"status": "error", "message": "News not found"}), 404

        # Format data - only return requested fields
        formatted_item = {
            "_id": str(item["_id"]),
            "title": item.get("title", ""),
            "summary": item.get("summary", ""),
            "original_content": item.get("original_content", ""),
            "original_date": item.get("metadata", {}).get("original_date", "")
        }

        client.close()
        return jsonify({
            "status": "success",
            "data": formatted_item
        })

    except Exception as e:
        client.close()
        return jsonify({"status": "error", "message": "Invalid news ID"}), 400

# ====================================================================
# ENDPOINT BARU 1: Daftar Laporan Keuangan (Semua Company)
# ====================================================================
@app.route('/api/reports/list/<year>/<period>', methods=['GET'])
def get_reports_list(year, period):
    logger.info(f"Request received: GET /api/reports/list/{year}/{period}")
    
    try:
        client = get_mongo_client()
        db = client["idx_financial_data_production"]
        
        # Debug: Log semua collections yang ada
        collections = db.list_collection_names()
        logger.info(f"Available collections: {collections}")

        search_query = request.args.get('search', '')
        try:
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 9))
        except ValueError:
            page = 1
            limit = 9

        collection_name = f"processed_reports_{year}_{period}"
        logger.info(f"Looking for collection: {collection_name}")

        if collection_name not in collections:
            logger.warning(f"Collection {collection_name} not found")
            client.close()
            return jsonify({
                "status": "error",
                "message": f"Data untuk {year} periode {period} tidak ditemukan.",
                "available_collections": collections,
                "data": [],
                "total_count": 0
            }), 404

        collection = db[collection_name]
        
        # Debug: Check total documents in collection
        total_docs = collection.count_documents({})
        logger.info(f"Total documents in {collection_name}: {total_docs}")

        query = {}
        if search_query:
            query['$or'] = [
                {'company_code': {'$regex': search_query, '$options': 'i'}},
                {'company_name': {'$regex': search_query, '$options': 'i'}}
            ]
            logger.info(f"Search query: {query}")

        total_count = collection.count_documents(query)
        logger.info(f"Documents matching query: {total_count}")
        
        skip = (page - 1) * limit
        reports = list(collection.find(query).skip(skip).limit(limit))
        logger.info(f"Retrieved {len(reports)} reports")

        # Convert ObjectId to string
        for report in reports:
            if '_id' in report:
                report['_id'] = str(report['_id'])

        client.close()
        return jsonify({
            "status": "success",
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "data": reports
        })
        
    except Exception as e:
        logger.error(f"Error in get_reports_list: {str(e)}")
        if 'client' in locals():
            client.close()
        return jsonify({"status": "error", "message": str(e)}), 500

# ====================================================================
# ENDPOINT BARU 2: Detail Laporan Keuangan (Per Company)
# ====================================================================
@app.route('/api/reports/detail/<company_code>/<year>/<period>', methods=['GET'])
def get_report_detail(company_code, year, period):
    logger.info(f"Request received: GET /api/reports/detail/{company_code}/{year}/{period}")
    
    try:
        client = get_mongo_client()
        db = client["idx_financial_data_production"]
        collection_name = f"processed_reports_{year}_{period}"
        
        # Debug: Log semua collections yang ada
        collections = db.list_collection_names()
        logger.info(f"Available collections: {collections}")
        logger.info(f"Looking for collection: {collection_name}")

        if collection_name not in collections:
            logger.warning(f"Collection {collection_name} not found")
            client.close()
            return jsonify({
                "status": "error",
                "message": f"Data untuk {year} periode {period} tidak ditemukan.",
                "available_collections": collections
            }), 404

        collection = db[collection_name]
        
        # Debug: Log company codes yang ada
        company_codes = collection.distinct('company_code')
        logger.info(f"Available company codes: {company_codes}")
        
        company_code_upper = company_code.upper()
        logger.info(f"Searching for company_code: {company_code_upper}")

        report = collection.find_one({'company_code': company_code_upper})
        
        if not report:
            logger.warning(f"Report for {company_code_upper} not found")
            client.close()
            return jsonify({
                "status": "error",
                "message": f"Laporan keuangan untuk {company_code_upper} tidak ditemukan di periode ini.",
                "available_companies": company_codes
            }), 404
        
        # Convert ObjectId to string
        if '_id' in report:
            report['_id'] = str(report['_id'])
            
        client.close()
        logger.info(f"Successfully retrieved report for {company_code_upper}")

        return jsonify({
            "status": "success",
            "data": report
        })

    except Exception as e:
        logger.error(f"Error in get_report_detail: {str(e)}")
        if 'client' in locals():
            client.close()
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '_main_':
    app.run(host='0.0.0.0', port=5000, debug=True)