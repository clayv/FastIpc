namespace CVV
{
    internal class InternalDisposable : Disposable
    {
        new internal bool Disposed { get { return base.Disposed; } }
        new internal object NoDisposeWhileLocked { get { return base.NoDisposeWhileLocked; } }
    }
}
