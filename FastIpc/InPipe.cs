using System;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;

namespace CVV
{
    internal class InPipe : MutexFreePipe
    {
        int m_LastMessageProcessed;
        int m_BufferCount;

        readonly Action<byte[]> m_OnMessage;

        public InPipe(string name, bool createBuffer, Action<byte[]> onMessage) : base(name, createBuffer)
        {
            m_OnMessage = onMessage;
            new Thread(Go).Start();
        }

        void Go()
        {
            int spinCycles = 0;
            while (true)
            {
                int? latestMessageID = GetLatestMessageID();
                if (latestMessageID == null) return;            // We've been disposed.

                if (latestMessageID > m_LastMessageProcessed)
                {
                    Thread.MemoryBarrier();    // We need this because of lock-free implementation						
                    byte[] msg = GetNextMessage();
                    if (msg == null) return;
                    if (msg.Length > 0 && m_OnMessage != null) m_OnMessage(msg);       // Zero-length msg will be a buffer continuation 
                    spinCycles = 1000;
                }
                if (spinCycles == 0)
                {
                    NewMessageSignal.WaitOne();
                    if (Disposed)
                    {
                        return;
                    }
                }
                else
                {
                    Thread.MemoryBarrier();    // We need this because of lock-free implementation		
                    spinCycles--;
                }
            }
        }

        unsafe int? GetLatestMessageID()
        {
            lock (NoDisposeWhileLocked)
                lock (Buffer.NoDisposeWhileLocked)
                    return (Disposed || Buffer.Disposed ? (int?)null : *((int*)Buffer.Pointer));
        }

        unsafe byte[] GetNextMessage()
        {
            m_LastMessageProcessed++;

            lock (NoDisposeWhileLocked)
            {
                if (Disposed)
                {
                    return null;
                }

                lock (Buffer.NoDisposeWhileLocked)
                {
                    if (Buffer.Disposed) return null;

                    byte* offsetPointer = Buffer.Pointer + Offset;
                    var msgPointer = (int*)offsetPointer;

                    int msgLength = *msgPointer;

                    Offset += MessageHeaderLength;
                    offsetPointer += MessageHeaderLength;

                    if (msgLength == 0)
                    {
                        Buffer.Accessor.Write(4, true);   // Signal that we no longer need file				
                        Buffer.Dispose();
                        string newName = Name + "." + ++m_BufferCount;
                        Buffer = new SafeMemoryMappedFile(MemoryMappedFile.OpenExisting(newName));
                        Offset = StartingOffset;
                        return new byte[0];
                    }

                    Offset += msgLength;

                    //MMF.Accessor.ReadArray (Offset, msg, 0, msg.Length);    // too slow			
                    var msg = new byte[msgLength];
                    Marshal.Copy(new IntPtr(offsetPointer), msg, 0, msg.Length);
                    return msg;
                }
            }
        }

        protected override void CleanUpResources()
        {
            try
            {
                NewMessageSignal.Set();
            }
            finally
            {
                base.CleanUpResources();
            }
        }
    }
}
