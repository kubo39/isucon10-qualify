import std;
import vibe.d;
import mysql;

enum : long { LIMIT = 20 }
enum : size_t { NAZOTTE_LIMIT = 50 }

__gshared string[string] dbInfo;
__gshared Json CHAIR_SEARCH_CONDITION;
__gshared Json ESTATE_SEARCH_CONDITION;

MySQLPool pool;

shared static this()
{
    dbInfo["host"] = environment.get("MYSQL_HOST", "127.0.0.1");
    dbInfo["port"] = environment.get("MYSQL_PORT", "3306");
    dbInfo["username"] = environment.get("MYSQL_USER", "isucon");
    dbInfo["password"] = environment.get("MYSQL_PASS", "isucon");
    dbInfo["database"] = environment.get("MYSQL_DBNAME", "isuumo");

    CHAIR_SEARCH_CONDITION = parseJsonString(readText("../fixture/chair_condition.json"));
    ESTATE_SEARCH_CONDITION = parseJsonString(readText("../fixture/estate_condition.json"));
}

static this()
{
    pool = new MySQLPool(dbInfo["host"],
                         dbInfo["username"],
                         dbInfo["password"],
                         dbInfo["database"],
                         dbInfo["port"].to!ushort,
                         uint.max /* max conn, change later */);
}

struct Chair
{
    long id;
    string name;
    string description;
    string thumbnail;
    long price;
    long height;
    long width;
    long depth;
    string color;
    string features;
    string kind;
    long popularity;
    long stock;
}

struct Estate
{
    long id;
    string name;
    string description;
    string thumbnail;
    string address;
    double latitude;
    double longitude;
    long rent;
    long doorHeight;
    long doorWidth;
    string features;
    long popularity;
}


auto startTransaction(Connection conn)
{
    struct Transaction
    {
        Connection conn;
        bool committed;
        bool rollbacked;

        @disable this(this);
        @disable this();

        alias conn this;

        this(Connection conn)
        {
            this.conn = conn;
            this.committed = false;
            this.rollbacked = false;
        }

        ~this()
        {
            if (!this.committed && !this.rollbacked)
            {
                this.conn.exec(`ROLLBACK`);
            }
        }

        void commit()
        {
            assert(!this.rollbacked);
            this.conn.exec(`COMMIT`);
            this.committed = true;
        }

        void rollback()
        {
            assert(!this.committed);
            this.conn.exec(`ROLLBACK`);
            this.rollbacked = true;
        }
    }

    auto tx = Transaction(conn);
    tx.exec(`START TRANSCTION`);
    return tx;
}

class IsuumoAPI
{
    @method(HTTPMethod.POST)
    @path("/initialize")
    Json initialize()
    {
        const sqlDir = buildPath("..", "mysql", "db");
        const paths = [
            "0_Schema.sql",
            "1_DummyEstateData.sql",
            "2_DummyChairData.sql"
        ];
        const pwd = getcwd();
        foreach (p; paths)
        {
            const sqlFile = buildPath(pwd, sqlDir, p).buildNormalizedPath;
            const cmd = "mysql -h %s -u %s -p %s -P %d %s < %s".format(
                dbInfo["host"],
                dbInfo["username"],
                dbInfo["password"],
                dbInfo["port"],
                dbInfo["database"],
                sqlFile);
            auto my = executeShell(cmd, null, Config.none, size_t.max, null, "/bin/bash");
            enforceHTTP(my.status == 0, HTTPStatus.internalServerError);
        }
        return Json(["language": Json("d")]);
    }

    @path("/api/chair/low_priced")
    Json getApiChairLowPriced()
    {
        const sql = `SELECT * FROM chair WHERE stock > 0 ORDER BY price ASC, id ASC LIMIT %d`.format(LIMIT);
        auto conn = pool.lockConnection;
        auto rows = conn.query(sql).array;
        auto arr = Json.emptyArray;
        foreach (row; rows)
        {
            Chair chair;
            row.toStruct(chair);
            arr ~= chair.serializeToJson;
        }
        return Json(["chairs": arr]);
    }

    @path("/api/chair/search")
    Json getApiChairSearch()
    {
        string[] searchQueries;
        string[] queryParams;

        if ("priceRangeId" in request.params && request.params["priceRangeId"].length > 0)
        {
            Json charPrice;
            long priceRangeId;
            try
            {
                priceRangeId = request.params["priceRangeId"].to!long;
                charPrice = CHAIR_SEARCH_CONDITION["price"]["ranges"][priceRangeId];
            }
            catch (ConvException)
            {
                logError("priceRangeID invalid: %d", priceRangeId);
                enforceBadRequest(false);
            }

            if (charPrice["min"].to!long != -1)
            {
                searchQueries ~= `price >= ?`;
                queryParams ~= charPrice["min"].get!string;
            }
            if (charPrice["max"].to!long != -1)
            {
                searchQueries ~= `price < ?`;
                queryParams ~= charPrice["max"].get!string;
            }
        }

        if ("heightRangeId" in request.params && request.params["heightRangeId"].length > 0)
        {
            Json chairHeight;
            long heightRangeId;
            try
            {
                heightRangeId = request.params["heightRangeId"].to!long;
                chairHeight = CHAIR_SEARCH_CONDITION["height"]["ranges"][heightRangeId];
            }
            catch (ConvException)
            {
                logError("heightRangeId invalid: %d", heightRangeId);
                enforceBadRequest(false);
            }

            if (chairHeight["min"].to!long != -1)
            {
                searchQueries ~= `height >= ?`;
                queryParams ~= chairHeight["min"].get!string;
            }
            if (chairHeight["max"].to!long != -1)
            {
                searchQueries ~= `height >= ?`;
                queryParams ~= chairHeight["max"].get!string;
            }
        }

        if ("widthRangeId" in request.params && request.params["widthRangeId"].length > 0)
        {
            Json chairWidth;
            long widthRangeId;
            try
            {
                widthRangeId = request.params["widthRangeId"].to!long;
                chairWidth = CHAIR_SEARCH_CONDITION["width"]["ranges"][widthRangeId];
            }
            catch (ConvException)
            {
                logError("widthRangeId invalid: %d", widthRangeId);
                enforceBadRequest(false);
            }

            if (chairWidth["min"].to!long != -1)
            {
                searchQueries ~= `width >= ?`;
                queryParams ~= chairWidth["min"].get!string;
            }
            if (chairWidth["max"].to!long != -1)
            {
                searchQueries ~= `width < ?`;
                queryParams ~= chairWidth["max"].get!string;
            }
        }

        if ("depthRangeId" in request.params  && request.params["depthRangeId"].length > 0)
        {
            Json chairDepth;
            long depthRangeId;
            try
            {
                depthRangeId = request.params["depthRangeId"].to!long;
                chairDepth = CHAIR_SEARCH_CONDITION["depth"]["ranges"][depthRangeId];
            }
            catch (ConvException)
            {
                logError("depthRangeId invalid: %d", depthRangeId);
                enforceBadRequest(false);
            }

            if (chairDepth["min"].to!long != -1)
            {
                searchQueries ~= `depth >= ?`;
                queryParams ~= chairDepth["min"].get!string;
            }
            if (chairDepth["max"].to!long != -1)
            {
                searchQueries ~= `depth < ?`;
                queryParams ~= chairDepth["max"].get!string;
            }
        }

        if ("kind" in request.params && request.params["kind"].length > 0)
        {
            searchQueries ~= `kind = ?`;
            queryParams ~= request.params["kind"];
        }
        if ("color" in request.params && request.params["color"].length > 0)
        {
            searchQueries ~= `color = ?`;
            queryParams ~= request.params["color"];
        }
        if ("features" in request.params && request.params["features"].length > 0)
        {
            foreach (condition; request.params["features"].split(","))
            {
                searchQueries ~= `features LIKE CONCTAT('%', ?, '%')`;
                queryParams ~= condition;
            }
        }
        if (searchQueries.length == 0)
        {
            logError("Search condition not found");
            enforceBadRequest(false);
        }
        searchQueries ~= `stock > 0`;

        long page;
        try
        {
            page = request.params["page"].to!long;
        }
        catch (ConvException e)
        {
            logError("Invalid format page parameter: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }

        long perPage;
        try
        {
            perPage = request.params["perPage"].to!long;
        }
        catch (ConvException e)
        {
            logError("Invalid format perPage parameter: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }

        string sqlPrefix = `SELECT * FROM chair WHERE `;
        string searchCondition = searchQueries.join(` AND `);
        string limitOffset = ` ORDER BY popularity DESC, id ASC LIMIT %d OFFSET %d`.format(perPage, perPage * page);
        string countPrefix = `SELECT COUNT(*) as count FROM chair WHERE `;

        auto conn = pool.lockConnection;
        auto value = conn.queryValue("%s%s".format(countPrefix, searchCondition));
        auto count = value.get.get!long;
        auto rows = conn.query("%s%s%s".format(sqlPrefix, searchCondition, limitOffset)).array;
        Json chairs = Json.emptyArray;
        foreach (row; rows)
        {
            Chair chair;
            row.toStruct(chair);
            chairs ~= chair.serializeToJson;
        }
        return Json(["count": Json(count), "chairs": chairs]);
    }

    @path("/api/chair/:id")
    Json getApiChairById(string _id)
    {
        ulong id;
        try
        {
            id = _id.to!ulong;
        }
        catch (ConvException e)
        {
            logError("Request parameter \"id\" parse error: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }

        auto conn = pool.lockConnection;
        auto row = conn.queryRow(`SELECT * FROM chair WHERE id = ?`, id);
        if (row.isNull)
        {
            logInfo("Requested id's chair not found: %d", id);
            enforceHTTP(false, HTTPStatus.notFound);
        }
        Chair chair;
        row.get.toStruct(chair);
        if (chair.stock <= 0)
        {
            logInfo("Requested id's chair is sold out: %d", id);
            enforceHTTP(false, HTTPStatus.notFound);
        }
        return chair.serializeToJson;
    }

    @path("/api/chair")
    void postApiChair()
    {
        if ("chairs" !in request.files)
        {
            logError("Failed to get form file");
            enforceBadRequest(false);
        }

        auto conn = pool.lockConnection;
        auto tx = startTransaction(conn);
        try
        {
            auto path = request.files["chairs"].tempPath.toString;
            foreach (row; csvReader!Chair(path.readText))
            {
                auto sql = `INSERT INTO chair(id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                tx.exec(sql, row.tupleof);
            }

            tx.commit;
        }
        catch (Exception e)
        {
            logError("Failed to commit tx: %s", e.toString.sanitize);
            tx.rollback;
            throw e;
        }

        status(201);
    }

    @path("/api/chair/buy/:id")
    void postApiChairBuyById(string _id)
    {
        auto j = request.json;
        if (j.type == Json.Type.undefined)
        {
            logError("Failed to parse body: %s", j);
            enforceBadRequest(false);
        }
        if ("email" !in j)
        {
            logError("post buy chair failed: email not found in r");
            enforceBadRequest(false);
        }

        long id;
        try
        {
            id = _id.to!long;
        }
        catch (ConvException e)
        {
            logError("post buy chair failed: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }

        auto conn = pool.lockConnection;
        auto tx = startTransaction(conn);
        try
        {
            auto chair = tx.queryRow(`SELECT * FROM chair WHERE id = ? AND stock > 0 FOR UPDATE`, id);
            if (chair.isNull)
            {
                enforceHTTP(false, HTTPStatus.notFound);
            }
            tx.exec(`UPDATE chair SET stock = stock - 1 WHERE id = ?`, id);
            tx.commit;
        }
        catch (Exception e)
        {
            logError("Failed to commit tx: %s", e.toString.sanitize);
            tx.rollback;
            throw e;
        }

        status(200);
    }

    @path("/api/chair/search/condition")
    Json getApiChairSearchCondition()
    {
        return CHAIR_SEARCH_CONDITION;
    }

    @path("/api/estate/low_priced")
    Json getApiEstateLowPriced()
    {
        string sql = `SELECT * FROM estate ORDER BY rent ASC, id ASC LIMIT %d`.format(LIMIT);
        auto conn = pool.lockConnection;
        auto rows = conn.query(sql).array;
        Json estates = Json.emptyArray;
        foreach (row; rows)
        {
            Estate estate;
            row.toStruct(estate);
            estates ~= estate.serializeToJson;
        }
        return Json(["estates": estates]);
    }

    @path("/api/estate/search")
    Json getApiEstateSearch()
    {
        string[] searchQueries;
        string[] queryParams;

        if ("doorHeightRangeId" in request.params && request.params["doorHeightRangeId"].length > 0)
        {
            auto doorHeightRangeId = request.params["doorHeightRangeId"].to!long;
            auto doorHeight = ESTATE_SEARCH_CONDITION["doorHeight"]["ranges"][doorHeightRangeId];
            if (!doorHeight)
            {
                logError("doorHeightRangeId invalid: %d", doorHeightRangeId);
                enforceBadRequest(false);
            }
            if (doorHeight["min"].to!long != -1)
            {
                searchQueries ~= `door_height >= ?`;
                queryParams ~= doorHeight["min"].get!string;
            }
            if (doorHeight["max"].to!long != -1)
            {
                searchQueries ~= `door_height < ?`;
                queryParams ~= doorHeight["max"].get!string;
            }
        }

        if ("doorWidthRangeId" in request.params && request.params["doorWidthRangeId"].length > 0)
        {
            auto doorWidthRangeId = request.params["doorWidthRangeId"].to!long;
            auto doorWidth = ESTATE_SEARCH_CONDITION["doorWidth"]["ranges"][doorWidthRangeId];
            if (!doorWidth)
            {
                logError("doorWidthRangeId invalid: %d", doorWidthRangeId);
                enforceBadRequest(false);
            }

            if (doorWidth["min"].to!long != -1)
            {
                searchQueries ~= `door_width >= ?`;
                queryParams ~= doorWidth["min"].get!string;
            }
            if (doorWidth["max"].to!long != -1)
            {
                searchQueries ~= `door_width < ?`;
                queryParams ~= doorWidth["max"].get!string;
            }
        }

        if ("rentRangeId" in request.params && request.params["rentRangeId"].length > 0)
        {
            auto rentRangeId = request.params["rentRangeId"].to!long;
            auto rent = ESTATE_SEARCH_CONDITION["rent"]["ranges"][rentRangeId];
            if (!rent)
            {
                logError("rentRangeId invalid: %d", rentRangeId);
                enforceBadRequest(false);
            }

            if (rent["min"].to!long != -1)
            {
                searchQueries ~= `rent >= ?`;
                queryParams ~= rent["min"].get!string;
            }
            if (rent["max"].to!long != -1)
            {
                searchQueries ~= `rent < ?`;
                queryParams ~= rent["max"].get!string;
            }
        }

        if ("features" in request.params && request.params["features"].length > 0)
        {
            string[] features = request.params["features"].split(',');
            foreach (condition; features)
            {
                searchQueries ~= `features LIKE CONCAT('%', ?, '%')`;
                queryParams ~= condition;
            }
        }

        if (searchQueries.length == 0)
        {
            logError("Search condition not found");
            enforceBadRequest(false);
        }

        long page;
        try
        {
            page = request.params["page"].to!long;
        }
        catch (ConvException e)
        {
            logError("Invalid format page parameter: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }
        long perPage;
        try
        {
            perPage = request.params["perPage"].to!long;
        }
        catch (ConvException e)
        {
            logError("Invalid format perPage parameter: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }

        auto sqlPrefix = `SELECT * FROM estate WHERE `;
        auto searchCondition = searchQueries.join(` AND `);
        auto limitOffset = ` ORDER BY popularity DESC, id ASC LIMIT %d OFFSET %d`.format(perPage, perPage * page);
        auto countPrefix = `SELECT COUNT(*) as count FROM estate WHERE `;

        auto conn = pool.lockConnection;
        auto value = conn.queryValue(`%s%s`.format(countPrefix, searchCondition));
        auto count = value.get.get!long;
        auto rows = conn.query(`%s%s%s`.format(sqlPrefix, searchCondition, limitOffset)).array;

        Json estates = Json.emptyArray;
        foreach (row; rows)
        {
            Estate estate;
            row.toStruct(estate);
            estates ~= estate.serializeToJson;
        }

        return Json(["count": Json(count), "estates": estates]);
    }

    @path("/api/estate/nazotte")
    Json postApiEstateNazotte()
    {
        Json j = request.json;
        if (j.type == Json.Type.undefined)
        {
            logError("post search estate nazotte failed coordinates not found");
            enforceBadRequest(false);
        }
        if ("coordinates" !in j)
        {
            logError("post search estate nazotte failed: coordinates are empty");
            enforceBadRequest(false);
        }
        Json[] coordinates = j["coordinates"].get!(Json[]);
        if (coordinates.length == 0)
        {
            logError("post search estate nazotte failed: coordinates are empty");
            enforceBadRequest(false);
        }

        long[] longitudes = coordinates.map!(c => c["longitude"].get!long).array;
        long[] latitudes = coordinates.map!(c => c["latitude"].get!long).array;

        Json boundingBox = Json.emptyObject;
        boundingBox["top_left"] = Json(["longitude": Json(longitudes.minElement),
                                        "latitude": Json(latitudes.minElement)]);
        boundingBox["bottom_right"] = Json(["longitude": Json(longitudes.maxElement),
                                            "latitude": Json(latitudes.maxElement)]);

        auto sql = `SELECT * FROM estate WHERE latitude <= ? AND latitude >= ? AND longitude <= ? AND longitude >= ? ORDER BY popularity DESC, id ASC`;

        auto conn = pool.lockConnection;
        auto rows = conn.query(sql,
                               latitudes.maxElement,
                               latitudes.minElement,
                               longitudes.maxElement,
                               longitudes.minElement).array;
        Estate[] estatesInPolygon;
        foreach (row; rows)
        {
            auto latitude = row[5].get!double;
            auto longitude = row[6].get!double;
            auto point = `'POINT(%f %f)'`.format(latitude, longitude);
            string coordinatesToText = `'POLYGON((%s))'`.format(coordinates.map!(c => "%f %f".format(latitude, longitude)).array.join(','));
            auto sql2 = `SELECT * FROM estate WHERE id = ? AND ST_Contains(ST_PolygonFromText(%s), ST_GeomFromText(%s))`.format(coordinatesToText, point);
            auto r = conn.queryRow(sql2, row[0].get!long);
            if (!r.isNull)
            {
                Estate estate;
                r.get.toStruct(estate);
                estatesInPolygon ~= estate;
            }
        }
        Json nazotteEstates = Json.emptyArray;
        foreach (inPolygon; estatesInPolygon.take(NAZOTTE_LIMIT))
        {
            nazotteEstates ~= inPolygon.serializeToJson;
        }
        return Json(["estates": nazotteEstates, "count": Json(nazotteEstates.length)]);
    }

    @path("/api/estate/:id")
    Json getApiEstateById(string _id)
    {
        long id;
        try
        {
            id = _id.to!long;
        }
        catch (ConvException e)
        {
            logError("Request parameter \"id\" parse error: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }
        auto conn = pool.lockConnection;
        auto row = conn.queryRow(`SELECT * FROM estate WHERE id = ?`, id);
        if (row.isNull)
        {
            logError("Reqeusted id's estate not found: %d", id);
            enforceHTTP(false, HTTPStatus.notFound);
        }

        Estate estate;
        row.get.toStruct(estate);
        return estate.serializeToJson;
    }

    @path("/api/estate")
    void postApiEstate()
    {
        if ("estates" in request.files)
        {
            logError("Failed to get form file");
            enforceBadRequest(false);
        }

        Connection conn = pool.lockConnection;
        auto tx = startTransaction(conn);
        try
        {
            auto path = request.files["estates"].tempPath.toString;
            foreach (row; csvReader(path.readText))
            {
                auto sql = `INSERT INTO estate(id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                tx.exec(sql, row.map!(to!string).array);
            }
            tx.commit;
       }
        catch (Exception e)
        {
            logError("Failed to commit tx: %s", e.toString.sanitize);
            tx.rollback;
            throw e;
        }
        status(201);
    }

    @path("/api/estate/req_doc/:id")
    void postApiEstateReqdocById(string _id)
    {
        Json j = request.json;
        if (j.type == Json.Type.undefined)
        {
            logError("Failed to parse body: %s", j);
            enforceBadRequest(false);
        }
        if ("email" !in j)
        {
            logError("post request document failed: email not found in request body");
            enforceBadRequest(false);
        }

        long id;
        try
        {
            id = _id.to!long;
        }
        catch (ConvException e)
        {
            logError("post request document failed: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }
        auto conn = pool.lockConnection;
        auto estate = conn.queryRow(`SELECT * FROM estate WHERE id = ?`, id);
        if (estate.isNull)
        {
            logError("Requested id's estate not found: %d", id);
            enforceHTTP(false, HTTPStatus.notFound);
        }

        status(200);
    }

    @path("/api/estate/search/condition")
    Json getApiEstateSearchCondition()
    {
        return ESTATE_SEARCH_CONDITION;
    }

    @path("/api/recommended_estate/:id")
    Json getApiRecommendedestateById(string _id)
    {
        long id;
        try
        {
            id = _id.to!long;
        }
        catch (ConvException e)
        {
            logError("Request parameter \"id\" parse error: %s", e.toString.sanitize);
            enforceBadRequest(false);
        }
        auto conn = pool.lockConnection;
        auto chair = conn.queryRow(`SELECT * FROM chair WHERE id = ?`, id);
        if (chair.isNull)
        {
            logError("Requested id's chair not found: %d", id);
            enforceHTTP(false, HTTPStatus.notFound);
        }

        auto w = chair.get[6].get!long;
        auto h = chair.get[5].get!long;
        auto d = chair.get[7].get!long;
        const sql = `SELECT * FROM estate WHERE (door_width >= ? AND door_height >= ?) OR (door_width >= ? AND door_height >= ?) OR (door_width >= ? AND door_height >= ?) OR (door_width >= ? AND door_height >= ?) OR (door_width >= ? AND door_height >= ?) OR (door_width >= ? AND door_height >= ?) ORDER BY popularity DESC, id ASC LIMIT %d`.format(LIMIT);
        auto rows = conn.query(sql, w, h, w, d, h, w, h, d, d, w, d, h).array;

        Json estates = Json.emptyArray;
        foreach (row; rows)
        {
            Estate estate;
            row.toStruct(estate);
            estates ~= estate.serializeToJson;
        }
        return Json(["estates": estates]);
    }
}

void main()
{
    auto router = new URLRouter;
    router.registerWebInterface(new IsuumoAPI);

    auto settings = new HTTPServerSettings;
    settings.port = environment.get("SERVER_PORT", "1323").to!short;
    settings.bindAddresses = ["0.0.0.0"];
    settings.sessionStore = new MemorySessionStore;
    listenHTTP(settings, router);
    runApplication();
}
