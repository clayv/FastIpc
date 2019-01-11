using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks;

namespace CVV.FastIpcTestConsole
{
    class Program
    {
        const string PIPE_NAME = "test";
        const string CLIENT = "client";

        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                if (args[0].Equals(CLIENT, StringComparison.InvariantCultureIgnoreCase))
                {
                    StartClient();
                }
                else if (args[0].Equals("server", StringComparison.InvariantCultureIgnoreCase))
                {
                    Console.WriteLine($"Server process ID: {Process.GetCurrentProcess().Id}\n");
                    using (FastIpc server = new FastIpc(PIPE_NAME, true, null))
                    {
                        bool runInSeparateProcess = true;
                        if (runInSeparateProcess)
                        {
                            string library = Assembly.GetExecutingAssembly().Location;
                            Process.Start("dotnet", $"{library} {CLIENT}");
                        }
                        else
                        {
                            StartClient();
                        }
                        Console.ReadKey();
                    }
                }
                return;
            }
            Console.WriteLine("Incorrect usage, must specify <client|server> as command line argument.");
        }

        static void StartClient()
        {
            Console.WriteLine($"Client process ID: {Process.GetCurrentProcess().Id}\n");
            Task.Run(Client).Wait();
            Console.WriteLine("\nPress any key to exit");
        }

        async static Task Client()
        {
            using (var channel = new FastIpc(PIPE_NAME, false, null))
            {
                Proxy<Foo> fooProxy = await channel.Activate<Foo>();

                int remoteProcessID = await fooProxy.Eval(foo => foo.ProcessID);
                Console.WriteLine($"Remote process ID: {remoteProcessID}");

                int sum = await fooProxy.Eval(f1 => f1.Add(2, 2));
                Console.WriteLine($"Sum: {sum}");

                int sum2 = await fooProxy.Eval(f1 => f1.AddAsync(3, 3));
                Console.WriteLine($"Async Sum: {sum2}");

                #region Automatic Marshaling Demonstration

                Proxy<Bar> barProxy = await channel.Activate<Bar>();
                Proxy<Foo> fooProxy2 = await barProxy.Eval(b => b.GetFoo());
                int sum3 = await fooProxy2.Eval(f2 => f2.Add(4, 4));
                Console.WriteLine($"Sum from foo proxy #2: {sum3}");

                await barProxy.Run(b => b.PrintFoo(fooProxy2));
                await barProxy.Run(b => b.PrintFoo(new Foo()));

                #endregion
            }
        }
    }

    class Foo
    {
        public int ProcessID => Process.GetCurrentProcess().Id;

        public int Add(int x, int y) => x + y;

        public async Task<int> AddAsync(int x, int y)
        {
            await Task.Delay(1000);
            return x + y;
        }
    }

    class Bar
    {
        public Proxy<Foo> GetFoo() => new Foo();

        public async Task PrintFoo(Proxy<Foo> foo)
        {
            int fooID = await foo.Eval(f => f.ProcessID);
            Console.WriteLine($"Foo's process ID is: {fooID}");
        }
    }
}
