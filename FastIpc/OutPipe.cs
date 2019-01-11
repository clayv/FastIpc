using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;

namespace CVV
{
    internal class OutPipe : MutexFreePipe
    {
        int m_MessageNumber;
        int m_BufferCount;
        readonly List<SafeMemoryMappedFile> m_OldBuffers = new List<SafeMemoryMappedFile>();
        public int PendingBuffers => m_OldBuffers.Count;

        public OutPipe(string name, bool createBuffer) : base(name, createBuffer)
        {
        }

        public unsafe void Write(byte[] data)
        {
            lock (NoDisposeWhileLocked)                 // If there are multiple threads, write just one message at a time
            {
                AssertSafe();
                if (data.Length > Length - Offset - 8)
                {
                    // Not enough space left in the shared memory buffer to write the message.
                    WriteContinuation(data.Length);
                }
                WriteMessage(data);
                NewMessageSignal.Set();    // Signal reader that a message is available
            }
        }

        unsafe void WriteMessage(byte[] block)
        {
            byte* ptr = Buffer.Pointer;
            byte* offsetPointer = ptr + Offset;

            var msgPointer = (int*)offsetPointer;
            *msgPointer = block.Length;

            Offset += MessageHeaderLength;
            offsetPointer += MessageHeaderLength;

            if (block != null && block.Length > 0)
            {
                //MMF.Accessor.WriteArray (Offset, block, 0, block.Length);   // Horribly slow. No. No. No.
                Marshal.Copy(block, 0, new IntPtr(offsetPointer), block.Length);
                Offset += block.Length;
            }

            // Write the latest message number to the start of the buffer:
            int* iptr = (int*)ptr;
            *iptr = ++m_MessageNumber;
        }

        void WriteContinuation(int messageSize)
        {
            // First, allocate a new buffer:		
            string newName = Name + "." + ++m_BufferCount;
            int newLength = Math.Max(messageSize * 10, MinimumBufferSize);
            var newFile = new SafeMemoryMappedFile(MemoryMappedFile.CreateNew(newName, newLength, MemoryMappedFileAccess.ReadWrite));
            Trace.WriteLine($"Allocated new buffer of {newLength} bytes");

            // Write a message to the old buffer indicating the address of the new buffer:
            WriteMessage(new byte[0]);

            // Keep the old buffer alive until the reader has indicated that it's seen it:
            m_OldBuffers.Add(Buffer);

            // Make the new buffer current:
            Buffer = newFile;
            Length = newFile.Length;
            Offset = StartingOffset;

            // Release old buffers that have been read:	
            foreach (var buffer in m_OldBuffers.Take(m_OldBuffers.Count - 1).ToArray())
            {
                lock (buffer.NoDisposeWhileLocked)
                {
                    if (!buffer.Disposed && buffer.Accessor.ReadBoolean(4))
                    {
                        m_OldBuffers.Remove(buffer);
                        buffer.Dispose();
                        Trace.WriteLine("Cleaned file");
                    }
                }
            }
        }

        protected override void CleanUpResources()
        {
            try
            {
                foreach (var buffer in m_OldBuffers)
                {
                    buffer.Dispose();
                }
            }
            finally
            {
                base.CleanUpResources();
            }
        }
    }
}
