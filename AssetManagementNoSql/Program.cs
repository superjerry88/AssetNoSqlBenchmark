using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace AssetManagementNoSql
{
    class Program
    {
        const string FilePath1 = @"C:\Users\Jerry\Downloads\7-8 mar.csv";
        const string FilePath2 = @"C:\Users\Jerry\Downloads\8-9 mar.csv";
        const string FilePath3 = @"C:\Users\Jerry\Downloads\9-10 mar.csv";
        const string FilePath4 = @"C:\Users\Jerry\Downloads\10-11 mar.csv";
        const string FilePath5 = @"C:\Users\Jerry\Downloads\11-12 mar.csv";
        static void Main(string[] args)
        {
           
            //Console.WriteLine("Press enter to start");
            //Console.ReadLine();
            //Method1();
            //Method2();
            //Method3(FilePath1);
            //Method3(FilePath2);
            //Method3(FilePath3);
            //Method3(FilePath4);
            Method3(FilePath5);
            //Count1();
            Count2();
            CountCondition();
            FindMax();
            Console.WriteLine("Press enter to end");
            Console.ReadLine();
        }


        private static void Method1()
        {
            var row = 172519;
            var stopwatch = Stopwatch.StartNew();
            DbHelper.AddDataV1(GetCsvData(FilePath1, row));
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Insert Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms \nTotal Data: {24*row} \nAverage{stopwatch.ElapsedMilliseconds / (24.0* row)}ms per record");
        }

        private static void Method2()
        {
            var row = 172519;
            var stopwatch = Stopwatch.StartNew();
            DbHelper.AddDataV2(GetCsvData2(FilePath2, row));
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Insert Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms \nTotal Data: {24 * row} \nAverage{stopwatch.ElapsedMilliseconds / (24.0 * row)}ms per record");
            GC.Collect();
        }

        private static void Method3(string file)
        {
            var row = 172519;
            var stopwatch = Stopwatch.StartNew();
            DbHelper.AddDataV3(GetCsvData2(file, row));
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Insert Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms \nTotal Data: {24 * row} \nAverage{stopwatch.ElapsedMilliseconds / (24.0 * row)}ms per record");
            GC.Collect();
        }

        private static void Count1()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = DbHelper.CountHourlyData("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }

        private static void Count2()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = DbHelper.CountHourlyData2("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Count, result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }

        private static void CountCondition()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = DbHelper.CountWithCondition("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Count Condition, result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }


        private static void FindMax()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = DbHelper.FindMax("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"FindMax, result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }

        public static Dictionary<string, List<Item>> GetCsvData(string filePath, int row)
        {
            using (var reader = new StreamReader(filePath))
            {
                var names = new List<string>();
                var items = new Dictionary<string, List<Item>>();

                for (var i = 0; i < row && !reader.EndOfStream; i++)
                {
                    var line = reader.ReadLine();
                    var values = line.Split('\t');
                    if (i == 0) names.AddRange(values);
                    else
                    {
                        for (var col = 0; col < values.Length; col++)
                        {
                            var stringValue = values[col];
                            var name = names[col].Replace("\"","");
                            if (name.Contains("Time")) continue;
                            if (!items.ContainsKey(name)) items[name] = new List<Item>();

                            var date = DateTime.Parse(values[0]);
                            items[name].Add(double.TryParse(stringValue, out var number) ? new Item(number, date) : new Item(stringValue, date));
                        }
                    }
                }
                return items;
            }
        }

        public static Dictionary<string, HourData> GetCsvData2(string filePath, int row)
        {
            using (var reader = new StreamReader(filePath))
            {
                var names = new List<string>();

                var asset = new Dictionary<string, HourData>();

                for (var i = 0; i < row && !reader.EndOfStream; i++)
                {
                    var line = reader.ReadLine();
                    var values = line.Split('\t');
                    if (i == 0)
                    {
                        names.AddRange(values);
                        foreach (var name in names.Where(c => !c.Contains("Time")))
                        {
                            DbHelper.CreateAssetIfNotExist(name.Replace("\"",""));
                        }
                    }
                    else
                    {
                        for (var col = 0; col < values.Length; col++)
                        {
                            var assetName = names[col].Replace("\"", "");
                            if (assetName.Contains("Time")) continue;

                            var date = DateTime.Parse(values[0]);
                            var stringValue = values[col];
                            var item = double.TryParse(stringValue, out var number) ? new Item(number, date) : new Item(stringValue, date);
                            var key = $"SensorData_{assetName}_{date:yyyy_MM_dd_HH}";

                            if (!asset.ContainsKey(key))
                            {
                                asset[key] = new HourData(assetName, item.DateTime);
                                DbHelper.AddHourlyDataKeyToAsset(assetName,key);
                            }
                            asset[key].Items.Add(item);
                        }
                    }
                }
                return asset;
            }
        }
    }

    
    public class DbHelper
    {
        public static void AddDataV1( Dictionary<string, List<Item>> items)
        {
            foreach (var itemsKey in items.Keys) CreateAssetIfNotExist(itemsKey);

            foreach (var data in items)
            {
                var hourlyData = new Dictionary<string, HourData>();
                foreach (var item in data.Value)
                {
                    var key = item.DateTime.ToString("yyyy_MM_dd_HH");
                    if(!hourlyData.ContainsKey(key)) hourlyData[key] = new HourData(data.Key,item.DateTime);

                    hourlyData[key].Items.Add(item);
                }

                foreach (var hourData in hourlyData)
                {
                    using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
                    {
                        session.Store(hourData.Value);
                        var id = hourData.Value.Id;
                        session.SaveChanges();

                        var asset = session.Load<SensorAsset>(hourData.Value.Asset);
                        asset.HourDatas.Add(id);
                        session.SaveChanges();
                        Console.WriteLine($"Added {data.Key}");
                    }
                }


            }
        }

        public static void AddDataV2(Dictionary<string, HourData> items)
        {
            Parallel.ForEach(items.Values, data =>
            {
                using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
                {
                    session.Store(data);
                    var id = data.Id;
                    session.SaveChanges();
                    Console.WriteLine($"Added {id}");
                }
            });
        }

        public static void AddDataV3(Dictionary<string, HourData> items)
        {
            using (var session = DocumentStoreHolder.Store.BulkInsert("TestDb1"))
            {
                foreach (var data in items.Values)
                {
                    session.Store(data);
                    var id = data.Id;
                    Console.WriteLine($"Added {id}");
                }
            }
        }

        public static int CountHourlyData(string assetId)
        {
            var count = 0;
            using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
            {
                var asset = session.Include<SensorAsset>(x => x.HourDatas).Load(assetId);
                foreach (var hourDataId in asset.HourDatas)
                {
                    var hourData = session.Load<HourData>(hourDataId);
                    count += hourData.Items.Count;
                }
            }
            return count;
        }

        public static int CountHourlyData2(string assetId)
        {
            var count = 0;
            using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
            {
                var asset = session.Load<SensorAsset>(assetId);
                foreach (var hourDataId in asset.HourDatas)
                {
                    using (var ss = DocumentStoreHolder.Store.OpenSession("TestDb1"))
                    {
                        var hourData = ss.Load<HourData>(hourDataId);
                        count += hourData.Items.Count;
                    }
                }
            }
            return count;
        }

        public static int CountWithCondition(string assetId)
        {
            var count = 0;
            using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
            {
                var asset = session.Load<SensorAsset>(assetId);
                foreach (var hourDataId in asset.HourDatas)
                {
                    using (var ss = DocumentStoreHolder.Store.OpenSession("TestDb1"))
                    {
                        var hourData = ss.Load<HourData>(hourDataId);
                        count += hourData.Items.Count(item => item.Value > 5);
                    }
                }
            }
            return count;
        }

        public static double FindMax(string assetId)
        {
            double max = 0.00;
            using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
            {
                var asset = session.Load<SensorAsset>(assetId);
                foreach (var hourDataId in asset.HourDatas)
                {
                    using (var ss = DocumentStoreHolder.Store.OpenSession("TestDb1"))
                    {
                        var hourData = ss.Load<HourData>(hourDataId);
                        var minimax = hourData.Items.Max(i => i.Value);
                        if (minimax > max) max = minimax;
                    }
                   
                }
            }
            return max;
        }

        public static void AddHourlyDataKeyToAsset(string assetid, string hourdataid)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
            {
                var asset = session.Load<SensorAsset>(assetid);
                asset.HourDatas.Add(hourdataid);
                session.SaveChanges();
            }
        }

        public static void CreateAssetIfNotExist(string asset)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession("TestDb1"))
            {
                if (session.Advanced.Exists(asset)) return;
                session.Store(new Asset
                {
                    Id = asset,
                    AssetId = asset
                });
                session.SaveChanges();
            }
        }


    }

    public class Asset
    {
        public string Id { get; set; }
        public string AssetId { get; set; }
        
    }

    public class SensorAsset:Asset
    {
        public HashSet<string> HourDatas { get; set; } = new HashSet<string>();
    }

    public class HourData
    {
        public string Id { get; set; }
        public string Asset { get; set; }
        public DateTime DateTime { get; set; }
        public List<Item> Items { get; set; } = new List<Item>();
        public HourData(string asset, DateTime dt)
        {
            Asset = asset;
            Id = $"SensorData_{asset}_{dt:yyyy_MM_dd_HH}";
            DateTime = dt;

        }
    }

    public class Item
    {
        public string Data { get; set; }
        public double Value { get; set; }
        public DateTime DateTime { get; set; }
        public bool IsValue { get; set; }
        public string GetData() => IsValue ? Value.ToString() : Data;

        public Item(string data, DateTime? dt = null)
        {
            var datetime = DateTime.Now;
            if (dt != null) datetime = (DateTime)dt;
            Data = data;
            DateTime = datetime;
            IsValue = false;
        }

        public Item(double data, DateTime? dt = null)
        {
            var datetime = DateTime.Now;
            if (dt != null) datetime = (DateTime)dt;
            Value = data;
            DateTime = datetime;
            IsValue = true;
        }

        public Item()
        {
        }
    }
}
