using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace JsonUpload
{
    class VideoInfo
    {
        enum ChildDirectoryType { parent, video, actorinfo, other };
        const string parentJsonFolder = @"parent";
        const string rawCollectionName = @"raw";
        const string videoInfoCollectionName = @"videos";
        const string actorInfoCollectionName = @"artists";
        const string videoRefCollectionName = @"videorefs";  // source_video_id, target_video_id, ref_by
        //const string rawCollectionName = @"raw"; // embedded all jsons in parent
        //const string watchStatisticsCollectionName = @"watchstats";  // year, month, day, videoid, userid, totalDuration, playDuration
        //const string videoStatisticsCollectionName = @"videostats";  // year, month, day, videoid, userid, watchCount, finishedCount
        string[] selectedFeatures = { "vid",
            "inuse",
            "vname",
            "valias",
            "videotype",
            "series",
            "updatenum",
            "maxclarity",
            "source",
            "duration",
            "storyplot",
            "issueyear",
            "studio",
            "issuedate",
            "showdate",
            "showtime",
            "tvstation",
            "boxoffice",
            "boxmoney",
            "taginfo",
            "score",
            "award",
            "branch",
            "model",
            "level",
            "price",
            "goodsname",
            "goodsid",
            "goodsbrand",
            "goodsprice",
            "orderphone",
            // following attributes are not in spec but in data
            "mtime",
            "ctime",
            "synctime",
            "isfinal"
        };

        string[] filterinfoFeatures = {
            "language",
            "category",
            "area",
            "musicstyle"
        };

        string[] selectedActorFeatures = {
            "actorid",
            "stagename",
            "fullname",
            "gender",
            "birthplace",
            "nationality",
            "taginfo",
            "birthdate",
            "refvideo",
            "representwork",
            "_role"
        };

        string[] actorTypes = {
            "director",
            "screenwriter",
            "star",
            "actor",
            "dubbing",
            "showhost",
            "showguest",
            "singer",
            "lyricist",
            "composer",
            "presenter"
        };

        const string jsonFileFilter = @"*.json";

        private IMongoCollection<BsonDocument> _colRaw;
        private IMongoCollection<BsonDocument> _colVideos;
        private IMongoCollection<BsonDocument> _colArtists;
        private IMongoCollection<BsonDocument> _colVideoRefs;

        public VideoInfo(string clientConnetionString, string dbName)
        {
            var client = new MongoClient(clientConnetionString);
            var db = client.GetDatabase(dbName);
            _colRaw = db.GetCollection<BsonDocument>(rawCollectionName);
            _colVideos = db.GetCollection<BsonDocument>(videoInfoCollectionName);
            _colArtists = db.GetCollection<BsonDocument>(actorInfoCollectionName);
            _colVideoRefs = db.GetCollection<BsonDocument>(videoRefCollectionName);
        }

        /// <summary>
        /// Read video info json files from a parent folder and generate a BsonDocument
        /// </summary>
        /// <param name="path">parent folder of video info, it should contain at least a parent folder and other optional folder like child, actorinfo, etc.</param>
        /// <returns></returns>
        public async Task<BsonDocument> ReadRawInfoFromPathAsync(string path)
        {
            // Read parent
            var parentPath = Path.Combine(path, parentJsonFolder);
            var files = GetJsonFileNames(parentPath);
            if (files.Length < 1)
            {
                return null;
            }

            var parentFileName = files.First();
            var parentJson = await ReadFromJsonFile(parentFileName);
            var doc = BsonSerializer.Deserialize<BsonDocument>(parentJson);

            foreach (var dir in Directory.GetDirectories(path))
            {
                var di = new DirectoryInfo(dir);
                if (di.Name == "parent")
                {
                    continue;
                }

                var children = new BsonArray();
                var jsonFiles = GetJsonFileNamesRecursively(dir);
                foreach (var file in jsonFiles)
                {
                    var childJson = await ReadFromJsonFile(file);
                    var childDoc = BsonSerializer.Deserialize<BsonDocument>(childJson);
                    if (di.Name == "actorinfo")
                    {
                        childDoc.Set("_role", GetParentFolderName(file));
                    }

                    children.Add(childDoc);
                }

                doc.Set("_" + di.Name, children);
            }

            return doc;
        }

        public void UploadAsync(BsonDocument doc, bool ifUploadRaw = true)
        {
            if (ifUploadRaw)
            {
                _colRaw.InsertOne(doc);
            }

            var videoFeatures = ExtractVideoFeatures(doc);
            _colVideos.InsertOne(videoFeatures);

            var artistList = new List<BsonDocument>();
            var refList = new List<BsonDocument>();
            ExtractArtistsRef(doc, artistList, refList);

            var upsert = new UpdateOptions() { IsUpsert = true };
            foreach (var a in artistList)
            {
                //var filter = Builders<BsonDocument>.Filter.Eq("actorid", a.GetValue("actorid"));
                //_colArtists.UpdateOne(filter, a, upsert);
                _colArtists.InsertOne(a);
            }

            foreach (var r in refList)
            {
                //var builder = Builders<BsonDocument>.Filter;
                //var filter = builder.And(
                //    builder.Eq("s_vid", r.GetValue("s_vid")),
                //    builder.Eq("t_vid", r.GetValue("t_vid")));
                //_colVideoRefs.UpdateOne(filter, r, upsert);
                _colVideoRefs.InsertOne(r);
            }
        }

        BsonDocument ExtractVideoFeatures(BsonDocument raw)
        {
            var doc = new BsonDocument();
            foreach (var feature in selectedFeatures)
            {
                if (raw.Names.Contains(feature))
                {
                    doc.SetElement(raw.GetElement(feature));
                }
            }

            // Flatten actorinfo
            var actorArray = new BsonArray();
            var actorDoc = raw.GetValue("actorinfo").AsBsonDocument;
            foreach (var role in actorDoc.Elements)
            {
                foreach (var id in role.Value.AsBsonArray)
                {
                    actorArray.Add(new BsonDocument {
                        { "role", role.Name },
                        { "id", id }
                    });
                }
            }

            doc.Set("actorinfo", actorArray);

            // Hoist filterinfo
            var filterinfo = raw.GetValue("filterinfo").AsBsonDocument;
            foreach (var filter in filterinfoFeatures)
            {
                doc.SetElement(filterinfo.GetElement(filter));
            }

            // Extract child video names and desription
            var childNameArray = new BsonArray();
            var childDescArray = new BsonArray();
            var children = raw.GetValue("_child").AsBsonArray;
            foreach (var child in children)
            {
                var cd = child.AsBsonDocument;
                childNameArray.Add(cd.GetValue("sname"));
                childDescArray.Add(cd.GetValue("storyplot"));
            }

            doc.Set("childnames", childNameArray);
            doc.Set("childplots", childDescArray);

            return doc;
        }

        void ExtractArtistsRef(BsonDocument raw, List<BsonDocument> artists, List<BsonDocument> videoRefs)
        {
            var vid = raw.GetValue("vid").AsInt32;
            var actorArray = raw.GetValue("_actorinfo").AsBsonArray;
            foreach (var actor in actorArray)
            {
                var src = actor.AsBsonDocument;
                var extracted = new BsonDocument();
                foreach (var f in selectedActorFeatures)
                {
                    extracted.SetElement(src.GetElement(f));
                }

                artists.Add(extracted);

                var role = src.GetValue("_role");
                var refVideoIds = src.GetValue("refvideo").AsBsonArray;
                foreach (var id in refVideoIds)
                {
                    var rid = id.AsInt32;
                    if (vid < rid)
                    {
                        videoRefs.Add(new BsonDocument
                        {
                            { "s_vid", vid },
                            { "t_vid", rid },
                            { "reftype", role }
                        });
                    }
                    else if (vid > rid)
                    {
                        videoRefs.Add(new BsonDocument
                        {
                            { "s_vid", rid },
                            { "t_vid", vid },
                            { "reftype", role }
                        });
                    }
                }
            }
        }

        BsonDocument String2BsonDocument(string jsonText)
        {
            return BsonSerializer.Deserialize<BsonDocument>(jsonText);
        }

        async Task<string> ReadFromJsonFile(string fileName)
        {
            using(var sr = new StreamReader(fileName))
            {
                return await sr.ReadToEndAsync();
            }
        }

        string[] GetJsonFileNames(string parent)
        {
            try
            {
                return Directory.GetFiles(parent, jsonFileFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: ", ex.Message);
                return new string[] {};
            }
        }

        string[] GetJsonFileNamesRecursively(string parent)
        {
            var fileName = new List<string>();
            try
            {
                foreach (string file in Directory.EnumerateFiles(parent, jsonFileFilter, SearchOption.AllDirectories))
                {
                    fileName.Add(file);
                }
            }
            catch (System.Exception excpt)
            {
                Console.WriteLine(excpt.Message);
                return new string[] { };
            }

            return fileName.ToArray();
        }

        string GetParentFolderName(string fileName)
        {
            var fi = new FileInfo(fileName);
            return fi.Directory.Name;
        }
    }
}
