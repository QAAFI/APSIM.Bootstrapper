using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using APSIM.Server.Commands;
using APSIM.Shared.Utilities;
using CommandLine;
using Microsoft.Rest;
using Models.Core.Replace;
using System.Linq;

namespace APSIM.Bootstrapper
{
    class Program
    {
        private static int exitCode = 0;

        static int Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            new Parser(config =>
            {
                config.AutoHelp = true;
                config.HelpWriter = Console.Out;
            }).ParseArguments<Options>(args)
              .WithParsed(Run)
              .WithNotParsed(HandleParseError);
            return exitCode;
        }

        /// <summary>
        /// Run the job manager.
        /// </summary>
        /// <param name="options">Options</param>
        private static void Run(Options options)
        {
            try
            {
                using (Bootstrapper bootstrapper = new Bootstrapper(options))
                {

                    // 1. Initialise the job.
                    bootstrapper.Initialise();

                    // Cleanup existing pods so we're starting from a known state.
                    bootstrapper.CleanupPods();

                    // Create the job manager.
                    bootstrapper.CreateJobManager();

                    ushort iWorkers = 60;
                    // Create some workers as a test.
                    IEnumerable<IPEndPoint> workers = bootstrapper.CreateWorkers(iWorkers, options.InputFile);

                    bootstrapper.StartJobmanager();
                    // bootstrapper.CreateDefaultSetup();
                    var iterations = new List<List<PropertyReplacement>>();
                    for(int i = 0; i < iWorkers; ++i)
                    {
                        iterations.Add(new List<PropertyReplacement>{
                            new PropertyReplacement("[Leaf].Parameters.tillerSdIntercept", "0.329"),
                            new PropertyReplacement("[Phenology].TTEndJuvToInit", "160"),
                            new PropertyReplacement("[Leaf].Parameters.aMaxSlope", "22.25")
                        });
                    // var listReplacements = new List<PropertyReplacement>{
                    //     new PropertyReplacement("[Leaf].Parameters.tillerSdIntercept", "0.329"),
                    //     new PropertyReplacement("[Phenology].TTEndJuvToInit", "160"),
                    //     new PropertyReplacement("[Leaf].Parameters.aMaxSlope", "22.25")
                        
                    };
                    var times = new List<(long cmd, long report)>();
                    int iReps = 100;
                    // Let's do this bit twice, just for fun.
                    for (int i = 0; i < iReps; i++)
                    {
                        var stopwatch = Stopwatch.StartNew();
                        // 2. Run everything.
                        //var command = new WGPCommand(iterations);
                        //var command = new RunCommand(listReplacements);
                        //RunCommand command = new RunCommand(new IReplacement[0]);
                        //bootstrapper.RunWithChanges(command);

                        //stopwatch.Stop();
                        //var cmdtime = stopwatch.ElapsedMilliseconds;
                        //stopwatch.Start();

                        // 3. Read outputs.
                        IEnumerable<string> parameters = new[]
                        {
                            "BiomassWt",
                            "Yield"
                        };
                        //removed "Date",
                        var readQuery = new WGPRelayCommand(iterations, "Report", parameters);
                        //var readQuery = new ReadQuery("Report", parameters);
                        var outputs = bootstrapper.ReadOutput(readQuery);
                        stopwatch.Stop();
                        var reporttime = stopwatch.ElapsedMilliseconds;
                        Console.WriteLine("Received output from cluster:");
                        //Console.WriteLine(DataTableUtilities.ToMarkdown(outputs, true));
                        foreach(var iter in outputs)
                        {
                            Console.WriteLine(iter.Join(","));
                        }
                        times.Add((reporttime, reporttime));
                    }

                    Console.WriteLine($"Ran {iReps} iterations with {iWorkers} workers in {times.Sum(t => t.cmd + t.report) / 1000}s");
                    foreach(var time in times)
                    {
                        Console.WriteLine($"Cmd: {time.cmd},  rep: {time.report}");
                    }

                    // next - test rerunning with changed inputs - should cause changed outputs
                    // bootstrapper.RunWithChanges(command);

                    // TestCopyFile.TestCompressToProcess();

                    // MoreTests.Run();
                }
            }
            catch (Exception err)
            {
                Console.Error.WriteLine(err);
                if (err is HttpOperationException httpError)
                    Console.Error.WriteLine(httpError.Response.Content);
                exitCode = 1;
            }
        }

        /// <summary>
        /// Handles parser errors to ensure that a non-zero exit code
        /// is returned when parse errors are encountered.
        /// </summary>
        /// <param name="errors">Parse errors.</param>
        private static void HandleParseError(IEnumerable<Error> errors)
        {
            if ( !(errors.IsHelp() || errors.IsVersion()) )
                exitCode = 1;
        }
    }
}
