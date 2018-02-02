using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

namespace JsonUpload
{
    class Program
    {
        const string localhost = @"mongodb://localhost:27017";
        const string dbName = "video_info";
        //const string testPath = @"D:\\Python\\DB\\example\\00\\11008";
        const string playbackEventFile = @"D:\\Python\\DB\\20180109-02.txt";
        const string playbackEventPartialFile = @"D:\\Python\\DB\\20180109-02-10lines.json";
        const string rootPath = @"D:\\videoinfo";
        //const string parentPath = @"D:\\videoinfo\\00";
        const string errorFile = @"D:\\videoinfo\\errorfile.log";

        static void Main(string[] args)
        {
            var db = new VideoInfo(localhost, dbName);

            foreach (var parentPath in Directory.GetDirectories(rootPath))
            {
                foreach (var path in Directory.GetDirectories(parentPath))
                {
                    try
                    {
                        Console.WriteLine("Parsing " + path);
                        var rawDoc = db.ReadRawInfoFromPathAsync(path).GetAwaiter().GetResult();
                        Console.WriteLine("Uploading");
                        db.UploadAsync(rawDoc, false);
                        Console.WriteLine(".");
                    }
                    catch (Exception ex)
                    {
                        PlaybackEvents.WriteLines(errorFile, new string[] { path, ex.Message, "" });
                    }
                }
            }
        }

        static void ExtractFirst10lines()
        {
            var lines = PlaybackEvents.ReadLines(playbackEventFile, 10);
            PlaybackEvents.WriteLines(playbackEventPartialFile, lines);
        }

        static void DeleteFolderRecursively(string directory)
        {
            ProcessStartInfo p = new ProcessStartInfo("cmd", "/c rmdir /s /q \"" + directory + "\"");
            p.WindowStyle = ProcessWindowStyle.Hidden; //hide mode
            Process.Start(p);
        }
    }
}
