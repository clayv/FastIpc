using System;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace CVV
{
    internal abstract class MutexFreePipe : Disposable
    {
        private const string INITIAL_FILE_EXT = ".0";

        protected const int MinimumBufferSize = 0x10000;
        protected readonly int MessageHeaderLength = sizeof(int);
        protected readonly int StartingOffset = sizeof(int) + sizeof(bool);

        public readonly string Name;
        protected readonly EventWaitHandle NewMessageSignal;
        protected SafeMemoryMappedFile Buffer;
        protected int Offset, Length;

        protected MutexFreePipe(string name, bool createBuffer)
        {
            Name = name;

            var mmFile = createBuffer
                ? MemoryMappedFile.CreateNew(String.Concat(name, INITIAL_FILE_EXT), MinimumBufferSize, MemoryMappedFileAccess.ReadWrite)
                : MemoryMappedFile.OpenExisting(String.Concat(name, INITIAL_FILE_EXT));

            Buffer = new SafeMemoryMappedFile(mmFile);
            NewMessageSignal = new EventWaitHandle(false, EventResetMode.AutoReset, String.Concat(name, ".signal"));

            Length = Buffer.Length;
            Offset = StartingOffset;
        }

        protected override void CleanUpResources()
        {
            try
            {
                Buffer.Dispose();
                NewMessageSignal.Dispose();
            }
            finally
            {
                base.CleanUpResources();
            }
        }
    }
}
