using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace TokenBasedAPI
{
    public class UserAuthentication : IDisposable
    {
        public string ValidateUser(string username, string password)
        {
            string Name = username == "Nash" ? "Valid" : "InValid";
            string Pass = password == "Nash" ? "Valid" : "InValid";

            if (Name == "Valid" && Pass == "Valid")
                return "true";
            else
                return "false";
        }
        public void Dispose()
        {
            //Dispose();
        }
    }
}