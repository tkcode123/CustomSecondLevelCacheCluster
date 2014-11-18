using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TestClusterModel;

namespace TestCluster
{
    class Program
    {
        static void Main(string[] args)
        {
            bool store = args.Length > 0 ? true : false;

            using (var ctx = TestClusterModelContext.Create("DB1",
                               typeof(CustomSecondLevelCacheClusterTransport.TcpCacheClusterTransport).AssemblyQualifiedName))
            {
                // Only needed the first time (or when the database model is changed)
                if (store)
                    ctx.UpdateSchema();

                ctx.Log = Console.Out;
                
                var p = ctx.Products.OrderByDescending(x => x.ID).FirstOrDefault();
                if (p == null)
                { 
                    p = new Product() { Price = 12.34m, ProductName = "TestCluster;0" };
                    ctx.Add(p);
                    ctx.SaveChanges();
                }

                while (true)
                {
                    Console.WriteLine("KEY (q to end)>>");
                    if (string.Equals(Console.ReadLine(), "q"))
                        break;
                    if (store)
                    {
                        var frags = p.ProductName.Split(';');
                        p.ProductName = "TestCluster;" + (1 + Int32.Parse(frags[1]));
                        Console.WriteLine("Storing {0}", p.ProductName);
                        ctx.SaveChanges();
                    }
                    Console.WriteLine("Seeing {0}", p.ProductName);
                    ctx.ClearChanges();
                }

                ctx.DisposeDatabase("Shutdown");
            }
        }

    }
}
