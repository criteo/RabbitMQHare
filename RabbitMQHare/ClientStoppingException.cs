using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQHare
{
    public class ClientStoppingException : Exception
    {
        public ClientStoppingException(string message) : base(message)
        {
        }
    }
}
