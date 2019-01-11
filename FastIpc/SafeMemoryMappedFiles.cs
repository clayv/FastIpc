using System.IO.MemoryMappedFiles;

namespace CVV
{
    internal class SafeMemoryMappedFile : InternalDisposable
    {
        readonly MemoryMappedFile m_MappedFile;
        readonly MemoryMappedViewAccessor m_Accessor;
        unsafe byte* m_Pointer;

        public int Length { get; private set; }

        public MemoryMappedViewAccessor Accessor
        {
            get { AssertSafe(); return m_Accessor; }
        }

        public unsafe byte* Pointer
        {
            get { AssertSafe(); return m_Pointer; }
        }

        public unsafe SafeMemoryMappedFile(MemoryMappedFile mmFile)
        {
            m_MappedFile = mmFile;
            m_Accessor = m_MappedFile.CreateViewAccessor();
            m_Pointer = (byte*)m_Accessor.SafeMemoryMappedViewHandle.DangerousGetHandle().ToPointer();
            Length = (int)m_Accessor.Capacity;
        }

        unsafe protected override void CleanUpResources()
        {
            try
            {
                m_Accessor.Dispose();
                m_MappedFile.Dispose();
                m_Pointer = null;
            }
            finally
            {
                base.CleanUpResources();
            }
        }
    }
}
