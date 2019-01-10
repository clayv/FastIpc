using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace CVV
{
    /// <summary>
    /// Wraps a reference to an object that potentially lives in another domain. This ensures that cross-domain calls are explicit in the
    /// source code, and allows for a transport other than .NET Remoting (e.g., FastChannel).
    /// </summary>
    public class Proxy<TRemote> : IProxy where TRemote : class
    {
        /// <summary>Any reference-type object can be implicitly converted to a proxy. The proxy will become connected
        /// automatically when it's serialized during a remote method call.</summary>
        public static implicit operator Proxy<TRemote>(TRemote instance)
        {
            if (instance == null) return null;
            return new Proxy<TRemote>(instance);
        }

        readonly object m_Lock = new object();

        /// <summary>The object being Remoted. This is populated only on the local side.</summary>
        public TRemote LocalInstance { get; private set; }

        /// <summary>This is populated if the proxy was obtained via FastChannel instead of Remoting.</summary>
        public FastIpc Channel { get; private set; }

        /// <summary>This is populated if the proxy was obtained via FastChannel instead of Remoting. It uniquely identifies the
        /// object within the channel if the object is connected (has a presence in the other domain).</summary>
        public int? ObjectID { get; private set; }

        /// <summary>Identifies the domain that owns the local instance, when FastChannel is in use.</summary>
        public byte DomainAddress { get; private set; }

        public bool IsDisconnected { get { return LocalInstance == null && Channel == null; } }

        Action m_OnDisconnect;
        readonly Type m_ActualObjectType;

        object IProxy.LocalInstanceUntyped { get { return LocalInstance; } }

        /// <summary>The real type of the object being proxied. This may be a subclass or derived implementation of TRemote.</summary>
        public Type ObjectType { get { return m_ActualObjectType ?? (LocalInstance != null ? LocalInstance.GetType() : typeof(TRemote)); } }

        Proxy(IProxy conversionSource, Action onDisconnect, Type actualObjectType)
        {
            LocalInstance = (TRemote)conversionSource.LocalInstanceUntyped;
            Channel = conversionSource.Channel;
            ObjectID = conversionSource.ObjectID;
            DomainAddress = conversionSource.DomainAddress;
            m_OnDisconnect = onDisconnect;
            m_ActualObjectType = actualObjectType;
        }

        public Proxy(TRemote instance) { LocalInstance = instance; }

        // Called via reflection:
        Proxy(TRemote instance, byte domainAddress) { LocalInstance = instance; DomainAddress = domainAddress; }

        // Called via reflection:
        Proxy(FastIpc channel, int objectID, byte domainAddress, Type actualInstanceType)
        {
            Channel = channel;
            ObjectID = objectID;
            DomainAddress = domainAddress;
            m_ActualObjectType = actualInstanceType;
        }

        public void AssertRemote()
        {
            if (LocalInstance != null)
            {
                throw new InvalidOperationException("Object " + LocalInstance.GetType().Name + " is not remote");
            }
        }

        /// <summary>Runs a (void) method on the object being proxied. This works on both the local and remote side.</summary>
        public Task Run(Expression<Action<TRemote>> remoteMethod)
        {
            var li = LocalInstance;
            if (li != null)
            {
                try
                {
                    var fastEval = FastIpc.FastEvalExpr<TRemote, object>(remoteMethod.Body);
                    if (fastEval != null) fastEval(li);
                    else remoteMethod.Compile()(li);
                    return Task.FromResult(false);
                }
                catch (Exception ex)
                {
                    return Task.FromException(ex);
                }
            }
            return SendMethodCall<object>(remoteMethod.Body, false);
        }

        /// <summary>Runs a (void) method on the object being proxied. This works on both the local and remote side.
        /// Use this overload for methods on the other domain that are themselves asynchronous.</summary>
        public Task Run(Expression<Func<TRemote, Task>> remoteMethod)
        {
            var li = LocalInstance;
            if (li != null)
            {
                try
                {
                    var fastEval = FastIpc.FastEvalExpr<TRemote, Task>(remoteMethod.Body);
                    if (fastEval != null) return fastEval(li);
                    return remoteMethod.Compile()(li);
                }
                catch (Exception ex)
                {
                    return Task.FromException(ex);
                }
            }
            return SendMethodCall<object>(remoteMethod.Body, true);
        }

        /// <summary>Runs a non-void method on the object being proxied. This works on both the local and remote side.</summary>
        public Task<TResult> Eval<TResult>(Expression<Func<TRemote, TResult>> remoteMethod)
        {
            var li = LocalInstance;
            if (li != null)
            {
                try
                {
                    var fastEval = FastIpc.FastEvalExpr<TRemote, TResult>(remoteMethod.Body);
                    if (fastEval != null) return Task.FromResult(fastEval(li));
                    return Task.FromResult(remoteMethod.Compile()(li));
                }
                catch (Exception ex)
                {
                    return Task.FromException<TResult>(ex);
                }
            }
            return SendMethodCall<TResult>(remoteMethod.Body, false);
        }

        /// <summary>Runs a non-void method on the object being proxied. This works on both the local and remote side.
        /// Use this overload for methods on the other domain that are themselves asynchronous.</summary>
        public Task<TResult> Eval<TResult>(Expression<Func<TRemote, Task<TResult>>> remoteMethod)
        {
            var li = LocalInstance;
            if (li != null)
                try
                {
                    var fastEval = FastIpc.FastEvalExpr<TRemote, Task<TResult>>(remoteMethod.Body);
                    if (fastEval != null) return fastEval(li);
                    return remoteMethod.Compile()(li);
                }
                catch (Exception ex)
                {
                    return Task.FromException<TResult>(ex);
                }
            return SendMethodCall<TResult>(remoteMethod.Body, true);
        }

        Task<TResult> SendMethodCall<TResult>(Expression expressionBody, bool awaitRemoteTask)
        {
            FastIpc channel;
            int? objectID;
            lock (m_Lock)
            {
                if (Channel == null)
                    return Task.FromException<TResult>(new InvalidOperationException("Channel has been disposed on Proxy<" + typeof(TRemote).Name + "> " + expressionBody));

                channel = Channel;
                objectID = ObjectID;
            }
            return channel.SendMethodCall<TResult>(expressionBody, objectID.Value, awaitRemoteTask);
        }

        /// <summary>This is useful when this.ObjectType is a subclass or derivation of TRemote.</summary>
        public Proxy<TNew> CastTo<TNew>() where TNew : class, TRemote
        {
            if (!typeof(TNew).IsAssignableFrom(ObjectType))
            {
                throw new InvalidCastException("Type '" + ObjectType.FullName + "' cannot be cast to '" + typeof(TNew).FullName + "'.");
            }

            return new Proxy<TNew>(this, m_OnDisconnect, m_ActualObjectType);
        }

        void IProxy.RegisterLocal(FastIpc fastChannel, int? objectID, Action onDisconnect)
        {
            // This is called by FastChannel to connect/register the proxy.
            lock (m_Lock)
            {
                Channel = fastChannel;
                ObjectID = objectID;
                DomainAddress = fastChannel.DomainAddress;
                m_OnDisconnect = onDisconnect;
            }
        }

        public void Disconnect()
        {
            Action onDisconnect;
            lock (m_Lock)
            {
                onDisconnect = m_OnDisconnect;
                m_OnDisconnect = null;
            }

            if (onDisconnect != null) onDisconnect();

            // If the remote reference drops away, we should ensure that it gets release on the other domain as well:

            lock (m_Lock)
            {
                if (Channel == null || LocalInstance != null || ObjectID == null)
                    LocalInstance = null;
                else
                    Channel.InternalDeactivate(ObjectID.Value);

                Channel = null;
                ObjectID = null;
            }
        }

        ~Proxy()
        {
            if (m_Lock != null)
                try
                {
                    Disconnect();
                }
                catch (ObjectDisposedException) { }
        }
    }
}
