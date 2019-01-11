using System.Linq;
using System.Threading.Tasks;

namespace CVV
{
    static class Extensions
    {
        public static object GetUntypedResult(this Task t)
        {
            if (!t.GetType().IsGenericType || t.GetType().GetGenericArguments().First().Name == "VoidTaskResult") return null;
            return ((dynamic)t).Result;
        }
    }
}
