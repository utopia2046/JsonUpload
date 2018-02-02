using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace JsonUpload
{
    class PlaybackEvents
    {
        // Read first N lines from a large file
        public static string[] ReadLines(string fileName, int lineNumber)
        {
            var lines = new string[lineNumber];
            using (var sr = new StreamReader(fileName))
            {
                for (var i = 0; i < lineNumber; i++)
                {
                    try
                    {
                        lines[i] = sr.ReadLine();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception: " + ex.Message);
                        break;
                    }
                }
            }

            return lines;
        }

        public static void WriteLines(string fileName, string[] lines)
        {
            using (var sw = new StreamWriter(fileName))
            {
                for (var i = 0; i < lines.Length; i++)
                {
                    sw.WriteLine(lines[i]);
                }
            }
        }
    }
}
