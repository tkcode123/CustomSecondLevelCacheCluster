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
            using (var ctx = TestClusterModelContext.Create(args.Length > 0 ? args[0] : "DB1", 
                               typeof(CustomSecondLevelCacheClusterTransport.RdmClusterTransport).AssemblyQualifiedName))
            {
                // Only needed the first time (or when the database model is changed)
                // ctx.UpdateSchema();

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
                    var frags = p.ProductName.Split(';');
                    p.ProductName = "TestCluster;"+(1+Int32.Parse(frags[1]));
                    Console.WriteLine("Storing {0}", p.ProductName);
                    ctx.SaveChanges();
                }

                ctx.DisposeDatabase("Shutdown");
            }
        }

    }
}
