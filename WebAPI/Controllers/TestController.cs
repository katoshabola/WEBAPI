using System.Linq;
using System.Web.Http;
using System.Security.Claims;
using System.Net.Http;
using System.Net;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using Newtonsoft.Json;
using System.Text;
using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using SAPbobsCOM;
using System.Threading.Tasks;
using System.Collections.Specialized;
using System.Data.Odbc;
using System.Text.RegularExpressions;
using System.Security.Cryptography;
using System.IO;
/// <summary>
namespace TokenBasedAPI.Controllers
{
    public class TestController : ApiController
    {

        public SAPbobsCOM.Company oCompany = null;
        private static int nErr = 0;
        string querystring = "";
        private static string erMsg = "";
        private static NameValueCollection section = (NameValueCollection)ConfigurationManager.GetSection("appSettings");
        HttpResponseMessage response = null;
        string message = "";

        private static string DbServerType = section["DbServerType"];
        private static string MessageFlag = section["MessageFlag"];
        private static string DBName = section["CompanyDB"];
        //private static string @"Database=" + DBName.Trim() + ";" = @"Database=" + DBName.Trim() + ";";
        private static string encrypt_decrypt_key = "b14ca5898a4e4133bbce2ea2315a1916";
        Recordset QueryObject = null;
        Recordset QueryObjectDocEntry = null;
        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetCurrencies")]
        public HttpResponseMessage GetCurrencies(string DBName)
        {
            try
            {
                var identity = (ClaimsIdentity)User.Identity;
                var roles = identity.Claims
                            .Where(c => c.Type == ClaimTypes.Role)
                            .Select(c => c.Value);
                string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
                using (OdbcConnection con = new OdbcConnection(constr))
                {

                   
                    if (DbServerType == "SAPHANA")
                        {
                         querystring = "select * FROM   \""  + Sanitize(DBName) + "\" + \".OCRN\"  ";

                         }
                    else
                        {
                         querystring = "select * FROM   "  + Sanitize(DBName) + ".[dbo]." + "OCRN  ";

                       }
                    using (OdbcCommand cmd = new OdbcCommand(querystring))
                    {
                        using (OdbcDataAdapter sda = new OdbcDataAdapter())
                        {
                            cmd.Connection = con;
                            sda.SelectCommand = cmd;
                            using (DataTable dt = new DataTable())
                            {
                                dt.TableName = "Dataset";
                                sda.Fill(dt);
                                //return dt;
                                 string results = DataTableToJSONWithStringBuilder(dt);
                                  var response = Request.CreateResponse(HttpStatusCode.OK);
                                response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                                return response;
                                // return Request.CreateResponse(HttpStatusCode.Created, customers);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }

        }
        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/GetSAPCompanies")]
        public HttpResponseMessage GetSAPCompanies()
        {
            try
            {
                var identity = (ClaimsIdentity)User.Identity;
                var roles = identity.Claims
                            .Where(c => c.Type == ClaimTypes.Role)
                            .Select(c => c.Value);
                string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
                using (OdbcConnection con = new OdbcConnection(constr))
                {

                    DBName = "[SBO-COMMON]";
                    if (DbServerType == "SAPHANA")
                        {
                         querystring = "select CompDbNam FROM   \""  + Sanitize(DBName) + "\" + \".SRGC\"  ";

                         }
                    else
                        {
                         querystring = "select * FROM   "  + Sanitize(DBName) + ".[dbo]." + "SRGC  ";

                       }
                    using (OdbcCommand cmd = new OdbcCommand(querystring))
                    {
                        using (OdbcDataAdapter sda = new OdbcDataAdapter())
                        {
                            cmd.Connection = con;
                            sda.SelectCommand = cmd;
                            using (DataTable dt = new DataTable())
                            {
                                dt.TableName = "Dataset";
                                sda.Fill(dt);
                                //return dt;
                                 string results = DataTableToJSONWithStringBuilder(dt);
                                  var response = Request.CreateResponse(HttpStatusCode.OK);
                                response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                                return response;
                                // return Request.CreateResponse(HttpStatusCode.Created, customers);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }

        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetIncomingPayments")]
       public HttpResponseMessage GetIncomingPayments(string DBName)
        {
            //try
            //{
                var identity = (ClaimsIdentity)User.Identity;
                var roles = identity.Claims
                            .Where(c => c.Type == ClaimTypes.Role)
                            .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString + @"Database=" + DBName.Trim() + ";";
         
           // string constr = (ConfigurationManager.ConnectionStrings["constr"].ConnectionString);
            //string constr = (DecryptString(encrypt_decrypt_key, "ics4rk5x7PdAhKGpSaI9EQ7QnVLudfbvP4/KbNqyZkPkdNy7+oxOCjueMpEDCpVjzFqWuH/kencobsf/QAIJOef+fFD+VLplL8ba96ECFeLmJaeg8hyK+ginTQRH5LTf"));
           // string constr = @"Driver ={ SQL Server}; Server = DESKTOP - NL6OLU8; Database = SBODEMOGB; Uid = sap; Pwd = sekonda1; MultipleActiveResultSets = True;";
            using (OdbcConnection con = new OdbcConnection(constr))
                {


                    if (DbServerType == "SAPHANA")
                    {
                        querystring = "select * FROM  " + "\"" + Sanitize(DBName) + "\"." + "\"ORCT\"  ";

                    }
                    else
                    {
                        querystring = "SELECT T0.DocEntry,T0.DocNum, T0.DocType,T0.Canceled, T0.Handwrtten, T0.Printed, T0.DocDate," +
                            " T0.DocDueDate, T0.CardCode, T0.CardName, T0.DdctPrcnt, T0.DdctSum, T0.DdctSumFC, T0.CashAcct, " +
                            "T0.CashSum, T0.CashSumFC, T0.CreditSum, T0.CredSumFC, T0.CheckAcct, T0.CheckSum, T0.CheckSumFC, T0.TrsfrAcct, " +
                            "T0.TrsfrSum, T0.TrsfrSumFC, T0.TrsfrDate, T0.TrsfrRef, T0.PayNoDoc, T0.NoDocSum, T0.NoDocSumFC, T0.DocCurr," +
                            " T0.DocRate, T0.SysRate, T0.DocTotal, T0.DocTotalFC, T0.Ref1, T0.Ref2, T0.CounterRef, T0.Comments, T0.JrnlMemo," +
                            " T0.TransId, T0.DocTime, T0.ShowAtCard, T0.CntctCode, T0.DdctSumSy, T0.CashSumSy, T0.CredSumSy, T0.CheckSumSy," +
                            " T0.TrsfrSumSy, T0.NoDocSumSy, T0.DocTotalSy, T0.StornoRate, T0.UpdateDate, T0.CreateDate, T0.TaxDate, T0.Series, " +
                            "T0.BankCode, T0.BankAcct, T0.VatGroup, T0.VatSum, T0.VatSumFC, T0.VatSumSy, T0.FinncPriod, T0.VatPrcnt, T0.Dcount," +
                            " T0.DcntSum, T0.DcntSumFC, T0.WtCode, T0.WtSum, T0.WtSumFrgn, T0.WtSumSys, T0.Proforma, T0.BpAct, T0.BcgSum, " +
                            "T0.BcgSumFC, T0.BcgSumSy, T0.PayToCode, T0.IsPaytoBnk, T0.PBnkCnt, T0.PBnkCode, T0.PBnkAccnt FROM "  + Sanitize(DBName) + ".[dbo]."+ "ORCT T0 ";

                    }
                    using (OdbcCommand cmd = new OdbcCommand(querystring))
                    {
                        using (OdbcDataAdapter sda = new OdbcDataAdapter())
                        {
                            cmd.Connection = con;
                            sda.SelectCommand = cmd;
                            using (DataTable dt = new DataTable())
                            {
                                dt.TableName = "Payments";
                                sda.Fill(dt);
                                //return dt;
                                 string results = DataTableToJSONWithStringBuilder(dt);
                                                               var response = Request.CreateResponse(HttpStatusCode.OK);
                                response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                                return response;
                                // return Request.CreateResponse(HttpStatusCode.Created, customers);
                            }
                        }
                    }


                }
            //}
            //catch (Exception ex)
            //{

            //    HttpResponseMessage exeption_response = null;
            //    exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
            //    return exeption_response;
            //}

        }



        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetOutgoingPayments")]
        public HttpResponseMessage GetOutgoingPayments(string DBName)
        {
            //try
            //{
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString + @"Database=" + DBName.Trim() + ";";

            // string constr = (ConfigurationManager.ConnectionStrings["constr"].ConnectionString);
            //string constr = (DecryptString(encrypt_decrypt_key, "ics4rk5x7PdAhKGpSaI9EQ7QnVLudfbvP4/KbNqyZkPkdNy7+oxOCjueMpEDCpVjzFqWuH/kencobsf/QAIJOef+fFD+VLplL8ba96ECFeLmJaeg8hyK+ginTQRH5LTf"));
            // string constr = @"Driver ={ SQL Server}; Server = DESKTOP - NL6OLU8; Database = SBODEMOGB; Uid = sap; Pwd = sekonda1; MultipleActiveResultSets = True;";
            using (OdbcConnection con = new OdbcConnection(constr))
            {


                if (DbServerType == "SAPHANA")
                {
                    querystring = "select * FROM  " + "\"" + Sanitize(DBName) + "\"." + "\"OVPM\"  ";

                }
                else
                {
                    querystring = "SELECT T0.DocEntry,T0.DocNum, T0.DocType,T0.Canceled, T0.Handwrtten, T0.Printed, T0.DocDate," +
                        " T0.DocDueDate, T0.CardCode, T0.CardName, T0.DdctPrcnt, T0.DdctSum, T0.DdctSumFC, T0.CashAcct, " +
                        "T0.CashSum, T0.CashSumFC, T0.CreditSum, T0.CredSumFC, T0.CheckAcct, T0.CheckSum, T0.CheckSumFC, T0.TrsfrAcct, " +
                        "T0.TrsfrSum, T0.TrsfrSumFC, T0.TrsfrDate, T0.TrsfrRef, T0.PayNoDoc, T0.NoDocSum, T0.NoDocSumFC, T0.DocCurr," +
                        " T0.DocRate, T0.SysRate, T0.DocTotal, T0.DocTotalFC, T0.Ref1, T0.Ref2, T0.CounterRef, T0.Comments, T0.JrnlMemo," +
                        " T0.TransId, T0.DocTime, T0.ShowAtCard, T0.CntctCode, T0.DdctSumSy, T0.CashSumSy, T0.CredSumSy, T0.CheckSumSy," +
                        " T0.TrsfrSumSy, T0.NoDocSumSy, T0.DocTotalSy, T0.StornoRate, T0.UpdateDate, T0.CreateDate, T0.TaxDate, T0.Series, " +
                        "T0.BankCode, T0.BankAcct, T0.VatGroup, T0.VatSum, T0.VatSumFC, T0.VatSumSy, T0.FinncPriod, T0.VatPrcnt, T0.Dcount," +
                        " T0.DcntSum, T0.DcntSumFC, T0.WtCode, T0.WtSum, T0.WtSumFrgn, T0.WtSumSys, T0.Proforma, T0.BpAct, T0.BcgSum, " +
                        "T0.BcgSumFC, T0.BcgSumSy, T0.PayToCode, T0.IsPaytoBnk, T0.PBnkCnt, T0.PBnkCode, T0.PBnkAccnt FROM " + Sanitize(DBName) + ".[dbo]." + "OVPM T0 ";

                }
                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Payments";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                            var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                            // return Request.CreateResponse(HttpStatusCode.Created, customers);
                        }
                    }
                }


            }
            //}
            //catch (Exception ex)
            //{

            //    HttpResponseMessage exeption_response = null;
            //    exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
            //    return exeption_response;
            //}

        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetCompanyDetails")]
        // [Route("api/SAP/GetIncomingPayments")]
        public HttpResponseMessage GetCompanyDetails(string DBName)
        {
            //try
            //{
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString + @"Database=" + DBName.Trim() + ";";

            // string constr = (ConfigurationManager.ConnectionStrings["constr"].ConnectionString);
            //string constr = (DecryptString(encrypt_decrypt_key, "ics4rk5x7PdAhKGpSaI9EQ7QnVLudfbvP4/KbNqyZkPkdNy7+oxOCjueMpEDCpVjzFqWuH/kencobsf/QAIJOef+fFD+VLplL8ba96ECFeLmJaeg8hyK+ginTQRH5LTf"));
            // string constr = @"Driver ={ SQL Server}; Server = DESKTOP - NL6OLU8; Database = SBODEMOGB; Uid = sap; Pwd = sekonda1; MultipleActiveResultSets = True;";
            using (OdbcConnection con = new OdbcConnection(constr))
            {


                if (DbServerType == "SAPHANA")
                {
                    querystring = "SELECT T0.CompnyName, T0.CompnyAddr, T0.Country, T0.Phone1, T0.Phone2, T0.E_Mail, T0.MainCurncy, T0.SysCurrncy, T0.TaxIdNum FROM " + Sanitize(DBName) + ".[dbo]." + "OADM T0";

                }
                else
                {
                    querystring = "SELECT T0.CompnyName, T0.CompnyAddr, T0.Country, T0.Phone1, T0.Phone2, T0.E_Mail, T0.MainCurncy, T0.SysCurrncy, T0.TaxIdNum FROM " + Sanitize(DBName) + ".[dbo]." + "OADM T0";
                       

                }
                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Payments";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                            var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                            // return Request.CreateResponse(HttpStatusCode.Created, customers);
                        }
                    }
                }


            }
            //}
            //catch (Exception ex)
            //{

            //    HttpResponseMessage exeption_response = null;
            //    exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
            //    return exeption_response;
            //}

        }





[Authorize(Roles = "SuperAdmin, Admin, User")]
[HttpGet]
[Route("api/SAP/{DBName}/GetIncomingPaymentsByDate/{FromDocDate}/{ToDocDate}")]
public HttpResponseMessage GetIncomingPaymentsByDate(string DBName ,string FromDocDate ,string ToDocDate)
{
    try
    {
        var identity = (ClaimsIdentity)User.Identity;
        var roles = identity.Claims
                    .Where(c => c.Type == ClaimTypes.Role)
                    .Select(c => c.Value);
        string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
        using (OdbcConnection con = new OdbcConnection(constr))
        {


            if (DbServerType == "SAPHANA")
            {
                querystring = "select * FROM   \""  + Sanitize(DBName) + "\" + \".ORCT\"  " +
                             " WHERE  \"DocDate\" BETWEEN '" + Sanitize(FromDocDate) + "' and  '" + Sanitize(ToDocDate) + "'";

                    }
            else
            {
                querystring = "select  T0.DocEntry,T0.DocNum, T0.DocType,T0.Canceled, T0.Handwrtten, T0.Printed, T0.DocDate," +
                            " T0.DocDueDate, T0.CardCode, T0.CardName, T0.DdctPrcnt, T0.DdctSum, T0.DdctSumFC, T0.CashAcct, " +
                            "T0.CashSum, T0.CashSumFC, T0.CreditSum, T0.CredSumFC, T0.CheckAcct, T0.CheckSum, T0.CheckSumFC, T0.TrsfrAcct, " +
                            "T0.TrsfrSum, T0.TrsfrSumFC, T0.TrsfrDate, T0.TrsfrRef, T0.PayNoDoc, T0.NoDocSum, T0.NoDocSumFC, T0.DocCurr," +
                            " T0.DocRate, T0.SysRate, T0.DocTotal, T0.DocTotalFC, T0.Ref1, T0.Ref2, T0.CounterRef, T0.Comments, T0.JrnlMemo," +
                            " T0.TransId, T0.DocTime, T0.ShowAtCard, T0.CntctCode, T0.DdctSumSy, T0.CashSumSy, T0.CredSumSy, T0.CheckSumSy," +
                            " T0.TrsfrSumSy, T0.NoDocSumSy, T0.DocTotalSy, T0.StornoRate, T0.UpdateDate, T0.CreateDate, T0.TaxDate, T0.Series, " +
                            "T0.BankCode, T0.BankAcct, T0.VatGroup, T0.VatSum, T0.VatSumFC, T0.VatSumSy, T0.FinncPriod, T0.VatPrcnt, T0.Dcount," +
                            " T0.DcntSum, T0.DcntSumFC, T0.WtCode, T0.WtSum, T0.WtSumFrgn, T0.WtSumSys, T0.Proforma, T0.BpAct, T0.BcgSum, " +
                            "T0.BcgSumFC, T0.BcgSumSy, T0.PayToCode, T0.IsPaytoBnk, T0.PBnkCnt, T0.PBnkCode, T0.PBnkAccnt FROM " + Sanitize(DBName) + ".[dbo]." + "ORCT T0 " +
                            " WHERE  DocDate  BETWEEN '" + Sanitize(FromDocDate) + "' and  '" + Sanitize(ToDocDate) + "'";

                    }
            using (OdbcCommand cmd = new OdbcCommand(querystring))
            {
                using (OdbcDataAdapter sda = new OdbcDataAdapter())
                {
                    cmd.Connection = con;
                    sda.SelectCommand = cmd;
                    using (DataTable dt = new DataTable())
                    {
                        dt.TableName = "Payments";
                        sda.Fill(dt);
                        //return dt;
                         string results = DataTableToJSONWithStringBuilder(dt);
                        var response = Request.CreateResponse(HttpStatusCode.OK);
                        response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                        return response;
                        // return Request.CreateResponse(HttpStatusCode.Created, customers);
                    }
                }
            }
        }
    }
    catch (Exception ex)
    {

        HttpResponseMessage exeption_response = null;
        exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
        return exeption_response;
    }

}

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetOutgoingPaymentsByDate/{FromDocDate}/{ToDocDate}")]
        public HttpResponseMessage GetOutgoingPaymentsByDate(string DBName, string FromDocDate, string ToDocDate)
        {
            try
            {
                var identity = (ClaimsIdentity)User.Identity;
                var roles = identity.Claims
                            .Where(c => c.Type == ClaimTypes.Role)
                            .Select(c => c.Value);
                string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString + @"Database=" + DBName.Trim() + ";";
                using (OdbcConnection con = new OdbcConnection(constr))
                {


                    if (DbServerType == "SAPHANA")
                    {
                        querystring = "select * FROM   \"" + Sanitize(DBName) + "\" + \".OVPM\"  " +
                                     " WHERE  \"DocDate\" BETWEEN '" + Sanitize(FromDocDate) + "' and  '" + Sanitize(ToDocDate) + "'";

                    }
                    else
                    {
                        querystring = "select  T0.DocEntry,T0.DocNum, T0.DocType,T0.Canceled, T0.Handwrtten, T0.Printed, T0.DocDate," +
                            " T0.DocDueDate, T0.CardCode, T0.CardName, T0.DdctPrcnt, T0.DdctSum, T0.DdctSumFC, T0.CashAcct, " +
                            "T0.CashSum, T0.CashSumFC, T0.CreditSum, T0.CredSumFC, T0.CheckAcct, T0.CheckSum, T0.CheckSumFC, T0.TrsfrAcct, " +
                            "T0.TrsfrSum, T0.TrsfrSumFC, T0.TrsfrDate, T0.TrsfrRef, T0.PayNoDoc, T0.NoDocSum, T0.NoDocSumFC, T0.DocCurr," +
                            " T0.DocRate, T0.SysRate, T0.DocTotal, T0.DocTotalFC, T0.Ref1, T0.Ref2, T0.CounterRef, T0.Comments, T0.JrnlMemo," +
                            " T0.TransId, T0.DocTime, T0.ShowAtCard, T0.CntctCode, T0.DdctSumSy, T0.CashSumSy, T0.CredSumSy, T0.CheckSumSy," +
                            " T0.TrsfrSumSy, T0.NoDocSumSy, T0.DocTotalSy, T0.StornoRate, T0.UpdateDate, T0.CreateDate, T0.TaxDate, T0.Series, " +
                            "T0.BankCode, T0.BankAcct, T0.VatGroup, T0.VatSum, T0.VatSumFC, T0.VatSumSy, T0.FinncPriod, T0.VatPrcnt, T0.Dcount," +
                            " T0.DcntSum, T0.DcntSumFC, T0.WtCode, T0.WtSum, T0.WtSumFrgn, T0.WtSumSys, T0.Proforma, T0.BpAct, T0.BcgSum, " +
                            "T0.BcgSumFC, T0.BcgSumSy, T0.PayToCode, T0.IsPaytoBnk, T0.PBnkCnt, T0.PBnkCode, T0.PBnkAccnt FROM " + Sanitize(DBName) + ".[dbo]." + "OVPM T0 " +
                            " WHERE  DocDate  BETWEEN '" + Sanitize(FromDocDate) + "' and  '" + Sanitize(ToDocDate) + "'";

                    }
                    using (OdbcCommand cmd = new OdbcCommand(querystring))
                    {
                        using (OdbcDataAdapter sda = new OdbcDataAdapter())
                        {
                            cmd.Connection = con;
                            sda.SelectCommand = cmd;
                            using (DataTable dt = new DataTable())
                            {
                                dt.TableName = "Payments";
                                sda.Fill(dt);
                                //return dt;
                                 string results = DataTableToJSONWithStringBuilder(dt);
                                var response = Request.CreateResponse(HttpStatusCode.OK);
                                response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                                return response;
                                // return Request.CreateResponse(HttpStatusCode.Created, customers);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }

        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetIncomingPaymentsByDocEntry/{DocEntry}")]
        public HttpResponseMessage GetIncomingPaymentsByDocEntry(string DBName, string DocEntry)
        {
            try
            {
                var identity = (ClaimsIdentity)User.Identity;
                var roles = identity.Claims
                            .Where(c => c.Type == ClaimTypes.Role)
                            .Select(c => c.Value);
                string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString + @"Database=" + DBName.Trim() + ";";
                using (OdbcConnection con = new OdbcConnection(constr))
                {


                    if (DbServerType == "SAPHANA")
                    {
                        querystring = "select * FROM   \""  + Sanitize(DBName) + "\" + \".ORCT\"  " +
                                     " WHERE  \"DocEntry\" = '" + Sanitize(DocEntry) + "'";

                    }
                    else
                    {
                        querystring = "select  T0.DocEntry,T0.DocNum, T0.DocType,T0.Canceled, T0.Handwrtten, T0.Printed, T0.DocDate," +
                            " T0.DocDueDate, T0.CardCode, T0.CardName, T0.DdctPrcnt, T0.DdctSum, T0.DdctSumFC, T0.CashAcct, " +
                            "T0.CashSum, T0.CashSumFC, T0.CreditSum, T0.CredSumFC, T0.CheckAcct, T0.CheckSum, T0.CheckSumFC, T0.TrsfrAcct, " +
                            "T0.TrsfrSum, T0.TrsfrSumFC, T0.TrsfrDate, T0.TrsfrRef, T0.PayNoDoc, T0.NoDocSum, T0.NoDocSumFC, T0.DocCurr," +
                            " T0.DocRate, T0.SysRate, T0.DocTotal, T0.DocTotalFC, T0.Ref1, T0.Ref2, T0.CounterRef, T0.Comments, T0.JrnlMemo," +
                            " T0.TransId, T0.DocTime, T0.ShowAtCard, T0.CntctCode, T0.DdctSumSy, T0.CashSumSy, T0.CredSumSy, T0.CheckSumSy," +
                            " T0.TrsfrSumSy, T0.NoDocSumSy, T0.DocTotalSy, T0.StornoRate, T0.UpdateDate, T0.CreateDate, T0.TaxDate, T0.Series, " +
                            "T0.BankCode, T0.BankAcct, T0.VatGroup, T0.VatSum, T0.VatSumFC, T0.VatSumSy, T0.FinncPriod, T0.VatPrcnt, T0.Dcount," +
                            " T0.DcntSum, T0.DcntSumFC, T0.WtCode, T0.WtSum, T0.WtSumFrgn, T0.WtSumSys, T0.Proforma, T0.BpAct, T0.BcgSum, " +
                            "T0.BcgSumFC, T0.BcgSumSy, T0.PayToCode, T0.IsPaytoBnk, T0.PBnkCnt, T0.PBnkCode, T0.PBnkAccnt FROM "  + Sanitize(DBName) + ".[dbo]." + "ORCT T0 " +
                            " WHERE  T0.DocEntry = '" + Sanitize(DocEntry) + "'";

                    }
                    using (OdbcCommand cmd = new OdbcCommand(querystring))
                    {
                        using (OdbcDataAdapter sda = new OdbcDataAdapter())
                        {
                            cmd.Connection = con;
                            sda.SelectCommand = cmd;
                            using (DataTable dt = new DataTable())
                            {
                                dt.TableName = "Dataset";
                                sda.Fill(dt);
                                //return dt;
                                 string results = DataTableToJSONWithStringBuilder(dt);
                               
                                var response = Request.CreateResponse(HttpStatusCode.OK);
                                response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                                return response;
                                // return Request.CreateResponse(HttpStatusCode.Created, customers);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }

        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetOutgoingPaymentsByDocEntry/{DocEntry}")]
        public HttpResponseMessage GetOutgoingPaymentsByDocEntry(string DBName, string DocEntry)
        {
            try
            {
                var identity = (ClaimsIdentity)User.Identity;
                var roles = identity.Claims
                            .Where(c => c.Type == ClaimTypes.Role)
                            .Select(c => c.Value);
                string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString + @"Database=" + DBName.Trim() + ";";
                using (OdbcConnection con = new OdbcConnection(constr))
                {


                    if (DbServerType == "SAPHANA")
                    {
                        querystring = "select * FROM   \"" + Sanitize(DBName) + "\" + \".OVPM\"  " +
                                     " WHERE  \"DocEntry\" = '" + Sanitize(DocEntry) + "'";

                    }
                    else
                    {
                        querystring = "select  T0.DocEntry,T0.DocNum, T0.DocType,T0.Canceled, T0.Handwrtten, T0.Printed, T0.DocDate," +
                            " T0.DocDueDate, T0.CardCode, T0.CardName, T0.DdctPrcnt, T0.DdctSum, T0.DdctSumFC, T0.CashAcct, " +
                            "T0.CashSum, T0.CashSumFC, T0.CreditSum, T0.CredSumFC, T0.CheckAcct, T0.CheckSum, T0.CheckSumFC, T0.TrsfrAcct, " +
                            "T0.TrsfrSum, T0.TrsfrSumFC, T0.TrsfrDate, T0.TrsfrRef, T0.PayNoDoc, T0.NoDocSum, T0.NoDocSumFC, T0.DocCurr," +
                            " T0.DocRate, T0.SysRate, T0.DocTotal, T0.DocTotalFC, T0.Ref1, T0.Ref2, T0.CounterRef, T0.Comments, T0.JrnlMemo," +
                            " T0.TransId, T0.DocTime, T0.ShowAtCard, T0.CntctCode, T0.DdctSumSy, T0.CashSumSy, T0.CredSumSy, T0.CheckSumSy," +
                            " T0.TrsfrSumSy, T0.NoDocSumSy, T0.DocTotalSy, T0.StornoRate, T0.UpdateDate, T0.CreateDate, T0.TaxDate, T0.Series, " +
                            "T0.BankCode, T0.BankAcct, T0.VatGroup, T0.VatSum, T0.VatSumFC, T0.VatSumSy, T0.FinncPriod, T0.VatPrcnt, T0.Dcount," +
                            " T0.DcntSum, T0.DcntSumFC, T0.WtCode, T0.WtSum, T0.WtSumFrgn, T0.WtSumSys, T0.Proforma, T0.BpAct, T0.BcgSum, " +
                            "T0.BcgSumFC, T0.BcgSumSy, T0.PayToCode, T0.IsPaytoBnk, T0.PBnkCnt, T0.PBnkCode, T0.PBnkAccnt FROM " + Sanitize(DBName) + ".[dbo]." + "OVPM T0 " +
                            " WHERE  T0.DocEntry = '" + Sanitize(DocEntry) + "'";

                    }
                    using (OdbcCommand cmd = new OdbcCommand(querystring))
                    {
                        using (OdbcDataAdapter sda = new OdbcDataAdapter())
                        {
                            cmd.Connection = con;
                            sda.SelectCommand = cmd;
                            using (DataTable dt = new DataTable())
                            {
                                dt.TableName = "Dataset";
                                sda.Fill(dt);
                                 string results = DataTableToJSONWithStringBuilder(dt);
                                var response = Request.CreateResponse(HttpStatusCode.OK);
                                response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                                return response;
                               }
                        }
                    }
                }
            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }

        }
        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetAccounts")]
        public HttpResponseMessage GetAccounts(string DBName)
        {
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {

                if (DbServerType == "SAPHANA")
                {
                    querystring = "select T0.\"AcctCode\", T0.\"AcctName\" from  \""  + Sanitize(DBName) + "\" + \".OACT\" T0 ";
                }
                else
                {
                    querystring = "select T0.AcctCode, T0.AcctName from  "  + Sanitize(DBName) + ".[dbo]." + "oact T0(nolock)";

                }
                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        cmd.CommandTimeout = 4000000;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                             var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                        }
                    }
                }
            }
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetTaxes")]
        public HttpResponseMessage GetTaxes(string DBName)
        {
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {


                if (DbServerType == "SAPHANA")
                {
                    querystring = "select T0.\"Code\", T0.\"Name\", T0.\"Inactive\",T0.\"Rate\" from  \""  + Sanitize(DBName) + "\" + \".OVTG\" T0  where T0.\"Inactive\"= 'N'";
                }
                else
                {
                    querystring = "select T0.Code, T0.Name, T0.Inactive,T0.Rate from  "  + Sanitize(DBName) + ".[dbo]." + "OVTG t0 (nolock) where  T0.Inactive='N'";

                }

                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                            var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                        }
                    }
                }
            }
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetWarehouses")]
        public HttpResponseMessage GetWarehouses(string DBName)
        {
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {

                if (DbServerType == "SAPHANA")
                {
                    querystring = "select T0.\"WhsCode\", T0.\"WhsName\" from  \""  + Sanitize(DBName) + "\" + \".OWHS\" T0  where T0.\"Locked\"= 'N'";
                }
                else
                {
                    querystring = "select T0.WhsCode, T0.WhsName from  "  + Sanitize(DBName) + ".[dbo]." + "OWHS t0 (nolock) where  T0.Locked='N'";

                }




                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                             var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                        }
                    }
                }
            }
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetPriceslist")]
        public HttpResponseMessage GetPriceslist(string DBName)
        {
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {

                if (DbServerType == "SAPHANA")
                {
                    querystring = "select *  from  \""  + Sanitize(DBName) + "\" + \".OPLN\"    ";
                }
                else
                {
                    querystring = "select * from  "  + Sanitize(DBName) + ".[dbo]." + "OPLN (nolock) ";

                }

                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                             var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                        }
                    }
                }
            }
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetItems")]
        public HttpResponseMessage GetItems(string DBName)
        {
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {

                if (DbServerType == "SAPHANA")
                {
                    querystring = "SELECT T0.*,T1.\"Price\",T1.\"PriceList\" FROM   \""  + Sanitize(DBName) + "\" + \".OITM\" T0  INNER JOIN   \""  + Sanitize(DBName) + "\" + \".ITM1\" T1 ON T0.\"ItemCode\" = T1.\"ItemCode\" where T0.\"SellItem\" = 'Y' AND T1.\"Price\" > 0 AND ManSerNum ='N' AND  T0.\"frozenFor\"='N')";

                }
                else
                {
                    querystring = "SELECT T0.*,T1.Price,T1.PriceList FROM   "  + Sanitize(DBName) + ".[dbo]." + "OITM T0(nolock) INNER JOIN   "  + Sanitize(DBName) + ".[dbo]." + "ITM1 T1 ON T0.ItemCode = T1.ItemCode where T0.SellItem ='Y' AND  T1.Price >0 AND ManSerNum ='N' AND frozenFor='N'";

                }
                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        cmd.CommandTimeout = 4000000;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                            var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");
                            return response;
                        }
                    }
                }
            }
        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/IsDBConnected")]
        public HttpResponseMessage Post39()
        {
           // try { 
            string message = "";
            //dynamic message_ = null;
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;

            using (OdbcConnection con = new OdbcConnection(constr))
            {
                con.Open();

                if (con.State == ConnectionState.Open)
                {
                    erMsg = "Database connection was sucessfull";
                    message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";
                    //  message_ = JsonConvert.DeserializeObject(message);
                }
                //  Interaction.MsgBox("Connected to Licence server successfully");
                else
                {
                    erMsg = "Database connection was not sucessfull";

                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";

                }
                con.Close();
                con.Dispose();
                // }
                message = message;
                var response = Request.CreateResponse(HttpStatusCode.OK);
                response.Content = new StringContent(JsonConvert.DeserializeObject(message).ToString(), Encoding.UTF8, "application/json");
                return response;
                MarshallObject(oCompany);
            //return response;
            //}

            //catch (Exception ex)
            //{
            //    // Console.WriteLine("Error writing app settings");
            //    message = ex.Message;
            }
            return response;
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
       // [Route("api/SAP/{DBName}/CheckGetPostStatus")]
        [Route("api/SAP/{DBName}/CheckGetPostStatus/{SAPUserName}/{SAPPassword}")]
        public HttpResponseMessage CheckGetPostStatus(string DBName, string SAPUserName, string SAPPassword)
        {
            //  try { 

            string message = "";
            dynamic dbstatus = null;


            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;

            using (OdbcConnection con = new OdbcConnection(constr))
            {
                con.Open();

                // , string SAPUserName, string SAPPassword
                AddUpdateAppSettings("CompanyDB", DBName);
                AddUpdateAppSettings("manager", SAPUserName);
                AddUpdateAppSettings("Password", SAPPassword);
                Connect_To_SAP connect = new Connect_To_SAP();
                oCompany = connect.ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                if ((0 == oCompany.Connect() && (con.State == ConnectionState.Open)))
                {
                    erMsg = "Both POST and GET method are ready ";
                    message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";

                }

                else if (0 != oCompany.Connect())
                {
                    int errcode;
                    oCompany.GetLastError(out nErr, out erMsg); 
                    if (!erMsg.Contains("already connected"))
                    {

                        message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";
                    }
                    else if (erMsg.Contains("already connected") && (con.State == ConnectionState.Open))
                    {

                        erMsg = "Both POST and GET method are ready ";
                        message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";


                    }

                }
                else
                {
                    erMsg = "GET method is not ready there is a problem with connection string";
                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";

                }
            }
            var response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content= new StringContent(JsonConvert.DeserializeObject(message).ToString(), Encoding.UTF8, "application/json"); 
            return response;
          

        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]

[Route("api/SAP/{DBName}/IsCompanyConnected/{SAPUserName}/{SAPPassword}")]
        public HttpResponseMessage IsCompanyConnected(string DBName, string SAPUserName, string SAPPassword)
        {
           
            try
            {
                //var response =null;
               
               // dynamic message_ = null;


                AddUpdateAppSettings("CompanyDB", DBName);
                AddUpdateAppSettings("manager", SAPUserName);
                AddUpdateAppSettings("Password", SAPPassword);

                Connect_To_SAP connect = new Connect_To_SAP();
                oCompany = connect.ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                if ((0 == oCompany.Connect()))
                {
                    erMsg = "You have successfully  connected to Company " + oCompany.CompanyName;
                    message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";
                    //  message_ = JsonConvert.DeserializeObject(message);
                }
                //  Interaction.MsgBox("Connected to Licence server successfully");
                else
                {
                    // Console.WriteLine("Error");
                    //  Interaction.MsgBox("failed  to connect  to to Licence server ");
                    int errcode;
                    oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                    if (erMsg.Contains("already connected"))
                    {
                        message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"" + erMsg + " " + oCompany.CompanyName + "\",\"Connection Status\": \"Company\"}}";
                    }
                    else
                    {
                        message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Connection Status\": \"Company\"}}";
                    }
                    
                }
                 response = Request.CreateResponse(HttpStatusCode.OK);
                response.Content = new StringContent(message, Encoding.UTF8, "application/json");
                MarshallObject(oCompany);
                
            }
           
               catch (Exception ex)
            {
                // Console.WriteLine("Error writing app settings");
                message = ex.Message;
            }
            return response;
        }

        static string AddUpdateAppSettings(string key, string value)
        {
            string message = "";
            try
            {
              
              //  var configFile = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
                Configuration configFile = System.Web.Configuration.WebConfigurationManager.OpenWebConfiguration("~");
                var settings = configFile.AppSettings.Settings;

                //value = EncryptString(encrypt_decrypt_key, value);
                // var settings_1 = configFile.ConnectionStrings.Settings;
                ConnectionStringSettings settings_ =
               ConfigurationManager.ConnectionStrings["name"];

                if (ConfigurationManager.AppSettings.AllKeys.Contains(key))
                {
                    //if (key.Contains("Password"))
                    //    {
                    //    //value = EncryptString(encrypt_decrypt_key,value);
                    //    value =  value;
                    //}
                    if (settings[key] == null)
                    {
                        settings.Add(key, value);
                    }
                    else
                    {
                        settings[key].Value = value;
                    }
                    configFile.Save(ConfigurationSaveMode.Modified);
                    ConfigurationManager.RefreshSection(configFile.AppSettings.SectionInformation.Name);
                  
                }
                else
                {

                    message = "Key does not Exist";
                }
            }
            catch (Exception ex)
            {
                // Console.WriteLine("Error writing app settings");
                message = ex.Message;
            }
            
            return message;
        }


        static string AddUpdateConfigurationSettings(string Name, string value)
        {
            string message = "";
            try
            {

                 Configuration configFile = System.Web.Configuration.WebConfigurationManager.OpenWebConfiguration("~");
                var settings = configFile.AppSettings.Settings;
               
                ConnectionStringSettings settings_ =   ConfigurationManager.ConnectionStrings[Name];
                              
                if (!(settings_ == null))
                {

                   // value = EncryptString(encrypt_decrypt_key, value);
                    // configFile.ConnectionStrings.ConnectionStrings.Clear(new ConnectionStringSettings(Name, value))
                    configFile.ConnectionStrings.ConnectionStrings["constr"].ConnectionString = value;
                    configFile.Save(ConfigurationSaveMode.Modified, true);
                    ConfigurationManager.RefreshSection("connectionStrings");
                }
                else
                {
                    //settings_[Name].Value = value;
                  
                    message = "Key does not Exist";
                }
               
            }
            catch (Exception ex)
            {
                // Console.WriteLine("Error writing app settings");
                message = ex.Message;
            }

            return message;
        }

        public static string EncryptString(string key, string plainText)
        {
            byte[] iv = new byte[16];
            byte[] array;

            using (Aes aes = Aes.Create())
            {
                aes.Key = Encoding.UTF8.GetBytes(key);
                aes.IV = iv;

                ICryptoTransform encryptor = aes.CreateEncryptor(aes.Key, aes.IV);

                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (CryptoStream cryptoStream = new CryptoStream((Stream)memoryStream, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter streamWriter = new StreamWriter((Stream)cryptoStream))
                        {
                            streamWriter.Write(plainText);
                        }

                        array = memoryStream.ToArray();
                    }
                }
            }

            return Convert.ToBase64String(array);
        }

        public static string DecryptString(string key, string cipherText)
        {
            byte[] iv = new byte[16];
            byte[] buffer = Convert.FromBase64String(cipherText);

            using (Aes aes = Aes.Create())
            {
                aes.Key = Encoding.UTF8.GetBytes(key);
                aes.IV = iv;
                ICryptoTransform decryptor = aes.CreateDecryptor(aes.Key, aes.IV);

                using (MemoryStream memoryStream = new MemoryStream(buffer))
                {
                    using (CryptoStream cryptoStream = new CryptoStream((Stream)memoryStream, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader streamReader = new StreamReader((Stream)cryptoStream))
                        {  var string_value = streamReader.ReadToEnd();
                            return string_value;
                        }
                    }
                }
            }
        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpPost]
        [Route("api/SAP/UpdateWebConfig/{type}/{key}/{value}")]
        public HttpResponseMessage UpdateWebConfig(string type,string key , string value)
        {
            string message = "";
            if  (type== "connectionStrings") {
            
                 message = AddUpdateConfigurationSettings(key, value);
               
            }
            else if  (type == "appSettings") 
            {
              
                 message = AddUpdateAppSettings(key, value);

            }

            
            if (string.IsNullOrEmpty(message))
            {
                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully updated configuration file\",\"Document Type\": \"configuration\"}}";

            }
            else
            {
                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"Key does not exist in the configuration file\",\"Document Type\": \"configuration\"}}";


            }
            var response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content = new StringContent(message, Encoding.UTF8, "application/json");
           // MarshallObject(oCompany);
            return response;
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetAvailableQuantity/{ItemCode}/{WarehouseCode}/{Quantity}")]
        public HttpResponseMessage GetAvailableQuantity(string DBName, string ItemCode, string WarehouseCode, string Quantity)
        {

            //ItemCode = ItemCode.Replace(";", "");
            //WarehouseCode = WarehouseCode.Replace(";", "");
            bool is_Numeric = IsNumeric(Quantity);
            //if (is_Numeric == true)
            //{ 

           
            if (DbServerType == "SAPHANA")
            {
                querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from  \""  + Sanitize(DBName) + "\" + \".OITM\" T0   INNER JOIN  \""  + Sanitize(DBName) + "\" + \".OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";

            }
            else
            {
                 querystring = "select case WHEN T1.OnHand  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS QuantityOk from " + Sanitize(DBName) + ".[dbo]." + "OITM T0  (nolock) INNER JOIN "  + Sanitize(DBName) + ".[dbo]." + "OITW  T1  ON T0.ItemCode = T1.ItemCode INNER JOIN "  + Sanitize(DBName) + ".[dbo]." + "OWHS T2 ON T2.WhsCode = T1.WhsCode WHERE  T0.ItemCode = '" + Sanitize(ItemCode) + "' and T2.WhsCode = '" + Sanitize(WarehouseCode) + "'";

            }
            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;

            //string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {
                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                               var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");

                            return response;
                        }
                    }
                }
            }
        }
            [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetItemPrice/{ItemCode}/{CardCode}")]
        public HttpResponseMessage GetItemPrice(string DBName,string ItemCode, string CardCode)
        {

            
            
            if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                querystring = "SELECT T0.\"ItemCode\", T0.\"Price\" ,T0.\"PriceList\" FROM  \""  + Sanitize(DBName) + "\" + \".ITM1\" T0 INNER JOIN  \""  + Sanitize(DBName) + "\" + \".OCRD\" T1 ON T0.\"PriceList\" = T1.\"ListNum\"  WHERE T1.\"CardCode\" = '" + Sanitize(CardCode) + "' AND  T0.\"ItemCode\"='" + Sanitize(ItemCode) + "'AND T0.\"Price\" != 0";

            }
            else
            {
                querystring = "SELECT T0.ItemCode, T0.Price ,T0.PriceList FROM  "  + Sanitize(DBName) + ".[dbo]." + "ITM1 T0 (nolock)INNER JOIN  "  + Sanitize(DBName) + ".[dbo]." + "OCRD T1 ON T0.PriceList = T1.ListNum  WHERE T1.CardCode = '" + Sanitize(CardCode) + "' AND  T0.ItemCode='" + Sanitize(ItemCode) + "'AND T0.Price <> 0";

            }

            var identity = (ClaimsIdentity)User.Identity;
            var roles = identity.Claims
                        .Where(c => c.Type == ClaimTypes.Role)
                        .Select(c => c.Value);
            string constr = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            using (OdbcConnection con = new OdbcConnection(constr))
            {
                using (OdbcCommand cmd = new OdbcCommand(querystring))
                {
                    using (OdbcDataAdapter sda = new OdbcDataAdapter())
                    {
                        cmd.Connection = con;
                        sda.SelectCommand = cmd;
                        using (DataTable dt = new DataTable())
                        {
                            dt.TableName = "Dataset";
                            sda.Fill(dt);
                            //return dt;
                             string results = DataTableToJSONWithStringBuilder(dt);
                          
                            var response = Request.CreateResponse(HttpStatusCode.OK);
                            response.Content = new StringContent(results, Encoding.UTF8, "application/json");

                            return response;
                        }
                    }
                }
            }

        }


        public class Connect_To_SAP
        {
            public SAPbobsCOM.Company vCompany = null;
            private static int errcode = 0;
            private static string erMsg = "";
            // public sap
            public SAPbobsCOM.Company ConnectSAPDB(string DBName, string SAPUserName, string SAPPassword)
            {


                vCompany = new SAPbobsCOM.Company();
                NameValueCollection section = (NameValueCollection)ConfigurationManager.GetSection("appSettings");
                string userName = section["userName"];  
                string Server = section["Server"];
                string DbServerType = section["DbServerType"];
                string LicenseServer = section["LicenseServer"];
                string UserName = SAPUserName;
                string Password = SAPPassword;
                    //section["Password"];
                //DecryptString(encrypt_decrypt_key, section["Password"]);
                string CompanyDB = DBName;
                    //section["CompanyDB"];
                string DbUserName = section["DbUserName"];
                string DbPassword = section["DbPassword"];
                //DecryptString(encrypt_decrypt_key, section["DbPassword"]);
                // string DbPassword = "sekonda";

                vCompany.Server = Server;
                // "DESKTOP-NL6OLU8";
                if (DbServerType == "MSSQL2012")
                { vCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_MSSQL2012; }
                else if (DbServerType == "MSSQL2014")
                { vCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_MSSQL2014; }
                else if (DbServerType == "MSSQL2016")
                { vCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_MSSQL2016; }
                else if (DbServerType == "MSSQL2019")
                { vCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_MSSQL2016; }
                else if (DbServerType == "SAPHANA")
                {
                    //vCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_MSSQL2014;
                    vCompany.DbServerType = SAPbobsCOM.BoDataServerTypes.dst_HANADB;
                }

                vCompany.LicenseServer = LicenseServer;
                //"DESKTOP-NL6OLU8:30000";
                vCompany.UserName = UserName;
                // "manager";
                vCompany.Password = Password;
                //"sekonda";
                //'SBODEMOGB
                //P@ssw0r#?
                vCompany.CompanyDB = CompanyDB;
                //"SBODEMOGB";
                vCompany.language = SAPbobsCOM.BoSuppLangs.ln_English;
                vCompany.DbUserName = DbUserName;
                //"sa";
                vCompany.DbPassword = DbPassword;
                //"sekonda";


                if ((0 == vCompany.Connect()))
                {
                    Console.WriteLine("success");
                }
                //  Interaction.MsgBox("Connected to Licence server successfully");
                else
                {
                    // Console.WriteLine("Error");
                    //  Interaction.MsgBox("failed  to connect  to to Licence server ");
                    
                    string message = "";
                    vCompany.GetLastError(out nErr, out erMsg);
                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": " + erMsg + ",\"Document Type\": \"Customer\"}}";
                  //  message_ = JsonConvert.DeserializeObject(message);
                }
                return vCompany;
            }

        }
        public class MarketingDocument_Rows
        {
            public string ItemCode { get; set; }
            public string Dscription { get; set; }
            public string Quantity { get; set; }
            public string WhsCode { get; set; }
            public string LineTotal { get; set; }
            public string PriceBefDi { get; set; }
            public string Price { get; set; }
            public string VatGroup { get; set; }
            public string VatSum { get; set; }
            public string DocEntry { get; set; }
        }

        public class MarketingDocumentHeader
        {
            public string CardCode { get; set; }
            public string CardName { get; set; }
            public string MarketingDocument { get; set; }
            public string DocDate { get; set; }
            public string DocDueDate { get; set; }
            public string TaxDate { get; set; }
            
            public string DocType { get; set; }
            public string CANCELED { get; set; }
            public string DocStatus { get; set; }
            public string DocTotal { get; set; }
            public string VatSum { get; set; }
            public string  DocDiscount { get; set; }
            public string DocNum { get; set; }
            public string DocEntry { get; set; }
            public List<MarketingDocument_Rows> MarketingDocument_Rows { get; set; }
        }

        


     [Authorize(Roles = "SuperAdmin, Admin, User")]
//[HttpPost]
[HttpGet]
[Route("api/SAP/{DBName}/GetMarketingDocument/{ObjectType}")]
public HttpResponseMessage GetMarketingDocument(string DBName ,string ObjectType)


{
            string Table = "";
            if (ObjectType == "SALESQOUTE")
            {
                 Table = "OQUT";
            }
            else if (ObjectType == "SALESORDER")
            {
            
                Table = "ORDR";
            }
            else if (ObjectType == "SALESCREDITNOTE")
            {

               
                Table = "ORIN";
            }
            else if (ObjectType == "SALESINVOICE")
            {
              
                Table = "OINV";

            }
            else if (ObjectType == "PURCHASEREQUEST")
            {
              
                Table = "OPRQ";

            }
            else if (ObjectType == "PURCHASEQOUTATION")
            {
                
                Table = "OPQT";
            }
            else if (ObjectType == "PURCHASEORDER")
            {
              
                Table = "OPOR";
            }
            else if (ObjectType == "PURCHASECREDITNOTE")
            {

               
                Table = "ORPC";
            }
            else if (ObjectType == "GRPO")
            {
             
                Table = "OPDN";

            }
            else if (ObjectType == "GOODSRETURN")
            {
               

                Table = "ORPD";
            }
            else if (ObjectType == "PURCHASEINVOICE")
            {
               
                Table = "OPCH";

            }

            List<MarketingDocumentHeader> invoices = new List<MarketingDocumentHeader>();


    if (DbServerType == "SAPHANA")
    {
        //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
        querystring = "select T0.\"DocEntry\",T0.\"DocNum\",T0.\"CardCode\", T0.\"CardName\", T0.\"DocDate\", T0.\"DocType\", T0.\"CANCELED\", T0.\"DocStatus\", T0.\"DocTotal\", T0.\"VatSum\" from  \"" + Sanitize(DBName) + "\" + \".OINV\" T0 ";

    }
    else
    {
        querystring = "select ISNULL(T0.DocEntry,'')'DocEntry',ISNULL(T0.DocNum,'') 'DocNum',ISNULL(T0.CardCode,'') 'CardCode', ISNULL(T0.CardName,'') 'CardName'," +
           " ISNULL( T0.DocDate,'') 'DocDate', ISNULL( T0.DocDueDate,'') 'DocDueDate', ISNULL( T0.TaxDate,'') 'TaxDate',ISNULL(T0.DocType,'') 'DocType', ISNULL(T0.CANCELED,'') 'CANCELED', ISNULL(T0.DocStatus ,'') 'DocStatus'," +
           " ISNULL(T0.DocTotal,0)'DocTotal', ISNULL(T0.VatSum,0)'VatSum',ISNULL(T0.DiscSum,0) 'DiscSum' from  " + Sanitize(DBName) + ".[dbo]." +  Table + " t0 (nolock) ";
        // querystring = "select  T0.DocEntry,T0.DocNum,T0.CardCode, T0.CardName, T0.DocDate, T0.DocType, T0.CANCELED, T0.DocStatus, ISNULL(T0.DocTotal,0), T0.VatSum ,ISNULL(T0.DiscSum,0) from  "  + Sanitize(DBName) + ".[dbo]." + "OINV t0 (nolock) ";


    }
       DataTable dt = GetData(querystring);
    for (int i = 0; i < dt.Rows.Count; i++)
    {

        MarketingDocumentHeader document = new MarketingDocumentHeader
        {


            DocEntry = Convert.ToString(dt.Rows[i]["DocEntry"])
            ,
            DocNum = Convert.ToString(dt.Rows[i]["DocNum"])
            ,

            CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
            ,
            CardName = Convert.ToString(dt.Rows[i]["CardName"])
            ,
            DocDate = Convert.ToString(dt.Rows[i]["DocDate"])

            ,
            MarketingDocument = ObjectType,
            DocDueDate = Convert.ToString(dt.Rows[i]["DocDueDate"])

            ,
            TaxDate = Convert.ToString(dt.Rows[i]["TaxDate"])

            ,
            DocType = Convert.ToString(dt.Rows[i]["DocType"])
            ,
            CANCELED = Convert.ToString(dt.Rows[i]["CANCELED"])
            ,
            DocStatus = Convert.ToString(dt.Rows[i]["DocStatus"])
            ,
            DocTotal = Convert.ToString(dt.Rows[i]["DocTotal"])
            ,
            VatSum = Convert.ToString(dt.Rows[i]["VatSum"])
            ,
            DocDiscount = Convert.ToString(dt.Rows[i]["DiscSum"])
            ,
            MarketingDocument_Rows = GetInvoiceRows(DBName, Convert.ToString(dt.Rows[i]["DocEntry"]))
        };
        invoices.Add(document);
    }

    var json = JsonConvert.SerializeObject(invoices, Newtonsoft.Json.Formatting.Indented);
    // return Request.CreateResponse(HttpStatusCode.Created, json);

    var response = Request.CreateResponse(HttpStatusCode.OK);
    response.Content = new StringContent(json, Encoding.UTF8, "application/json");
    return response;
}

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetInvoices")]
        public HttpResponseMessage GetInvoices(string DBName)


        {
            List<MarketingDocumentHeader> invoices = new List<MarketingDocumentHeader>();


              if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                querystring = "select T0.\"DocEntry\",T0.\"DocNum\",T0.\"CardCode\", T0.\"CardName\", T0.\"DocDate\", T0.\"DocType\", T0.\"CANCELED\", T0.\"DocStatus\", T0.\"DocTotal\", T0.\"VatSum\" from  \""  + Sanitize(DBName) + "\" + \".OINV\" T0 ";

            }
            else
            {
                querystring = "select ISNULL(T0.DocEntry,'')'DocEntry',ISNULL(T0.DocNum,'') 'DocNum',ISNULL(T0.CardCode,'') 'CardCode', ISNULL(T0.CardName,'') 'CardName'," +
                   " ISNULL( T0.DocDate,'') 'DocDate', ISNULL(T0.DocType,'') 'DocType', ISNULL(T0.CANCELED,'') 'CANCELED', ISNULL(T0.DocStatus ,'') 'DocStatus'," +
                   " ISNULL(T0.DocTotal,0)'DocTotal', ISNULL(T0.VatSum,0)'VatSum',ISNULL(T0.DiscSum,0) 'DiscSum' from  "  + Sanitize(DBName) + ".[dbo]." + "OINV t0 (nolock) ";
                // querystring = "select  T0.DocEntry,T0.DocNum,T0.CardCode, T0.CardName, T0.DocDate, T0.DocType, T0.CANCELED, T0.DocStatus, ISNULL(T0.DocTotal,0), T0.VatSum ,ISNULL(T0.DiscSum,0) from  "  + Sanitize(DBName) + ".[dbo]." + "OINV t0 (nolock) ";


            }
            DataTable dt = GetData(querystring);
            for (int i = 0; i < dt.Rows.Count; i++)
            {

                MarketingDocumentHeader invoice = new MarketingDocumentHeader
                {


                    DocEntry = Convert.ToString(dt.Rows[i]["DocEntry"])
                    ,
                    DocNum = Convert.ToString(dt.Rows[i]["DocNum"])
                    ,

                    CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
                    ,
                    CardName = Convert.ToString(dt.Rows[i]["CardName"])
                    ,
                    DocDate = Convert.ToString(dt.Rows[i]["DocDate"])

                    ,
                    DocType = Convert.ToString(dt.Rows[i]["DocType"])
                    ,
                    CANCELED = Convert.ToString(dt.Rows[i]["CANCELED"])
                    ,
                    DocStatus = Convert.ToString(dt.Rows[i]["DocStatus"])
                    ,
                    DocTotal = Convert.ToString(dt.Rows[i]["DocTotal"])
                    ,
                    VatSum = Convert.ToString(dt.Rows[i]["VatSum"])
                    ,
                    DocDiscount = Convert.ToString(dt.Rows[i]["DiscSum"])
                    ,
                    MarketingDocument_Rows = GetInvoiceRows(DBName, Convert.ToString(dt.Rows[i]["DocEntry"]))
                };
                invoices.Add(invoice);
            }

            var json = JsonConvert.SerializeObject(invoices, Newtonsoft.Json.Formatting.Indented);
            // return Request.CreateResponse(HttpStatusCode.Created, json);

            var response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content = new StringContent(json, Encoding.UTF8, "application/json");
            return response;
        }




        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetInvoicesByDate/{FromDocDate}/{ToDocDate}")]
        public HttpResponseMessage GetInvoicesByDate(string DBName,string FromDocDate  , string ToDocDate)


        {
            List<MarketingDocumentHeader> invoices = new List<MarketingDocumentHeader>();


            if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0  
                //INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\"" +
                  
                querystring = "select T0.\"DocEntry\",T0.\"DocNum\",T0.\"CardCode\", T0.\"CardName\", T0.\"DocDate\", T0.\"DocType\", T0.\"CANCELED\", T0.\"DocStatus\", T0.\"DocTotal\", T0.\"VatSum\" from  \""  + Sanitize(DBName) + "\" + \".OINV\" T0 " +
                       " WHERE  T0.\"DocDate\" BETWEEN '" + Sanitize(FromDocDate) + "' and  '" + Sanitize(ToDocDate) + "'";

            }
            else
            {
                querystring = "select  T0.DocEntry,T0.DocNum,T0.CardCode, T0.CardName, T0.DocDate, T0.DocType, T0.CANCELED, T0.DocStatus, T0.DocTotal, T0.VatSum from  "  + Sanitize(DBName) + ".[dbo]." + "OINV t0 (nolock) " +
                             " WHERE  T0.DocDate  BETWEEN '" + Sanitize(FromDocDate) + "' and  '" + Sanitize(ToDocDate) + "'";

            }

            DataTable dt = GetData( querystring);
            for (int i = 0; i < dt.Rows.Count; i++)
            {

                MarketingDocumentHeader invoice = new MarketingDocumentHeader
                {


                    DocEntry = Convert.ToString(dt.Rows[i]["DocEntry"])
                    ,
                    DocNum = Convert.ToString(dt.Rows[i]["DocNum"])
                    ,

                    CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
                    ,
                    CardName = Convert.ToString(dt.Rows[i]["CardName"])
                    ,
                    DocDate = Convert.ToString(dt.Rows[i]["DocDate"])

                    ,
                    DocType = Convert.ToString(dt.Rows[i]["DocType"])
                    ,
                    CANCELED = Convert.ToString(dt.Rows[i]["CANCELED"])
                    ,
                    DocStatus = Convert.ToString(dt.Rows[i]["DocStatus"])
                    ,
                    DocTotal = Convert.ToString(dt.Rows[i]["DocTotal"])
                    ,
                    VatSum = Convert.ToString(dt.Rows[i]["VatSum"])
                    ,
                    MarketingDocument_Rows = GetInvoiceRows( DBName,Convert.ToString(dt.Rows[i]["DocEntry"]))
                };
                invoices.Add(invoice);
            }

            var json = JsonConvert.SerializeObject(invoices, Newtonsoft.Json.Formatting.Indented);
            // return Request.CreateResponse(HttpStatusCode.Created, json);

            var response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content = new StringContent(json, Encoding.UTF8, "application/json");
            return response;
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetInvoicesById/{DocEntry}")]
        public HttpResponseMessage GetInvoicesById(string DBName,string DocEntry)

        {
            

            List<MarketingDocumentHeader> invoices = new List<MarketingDocumentHeader>();

            if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                querystring = "select top 1 T0.\"DocEntry\",T0.\"DocNum\",T0.\"CardCode\", T0.\"CardName\", T0.\"DocDate\", T0.\"DocType\", T0.\"CANCELED\", T0.\"DocStatus\", T0.\"DocTotal\", T0.\"VatSum\" from  \""  + Sanitize(DBName) + "\" + \".OINV\" T0  where T0.\"DocEntry\" ='" + Sanitize(DocEntry) + "'";

            }
            else
            {
                querystring = "select top 1 T0.DocEntry,T0.DocNum,T0.CardCode, T0.CardName, T0.DocDate, T0.DocType, T0.CANCELED, T0.DocStatus, ISNULL(T0.DocTotal,0) 'DocTotal', T0.VatSum ,ISNULL(T0.DiscSum,0) 'DiscSum' from  "  + Sanitize(DBName) + ".[dbo]." + "OINV t0 (nolock)  where T0.DocEntry ='" + Sanitize(DocEntry) + "'";

            }

            DataTable dt = GetData( querystring);
            for (int i = 0; i < dt.Rows.Count; i++)
            {

                MarketingDocumentHeader invoice = new MarketingDocumentHeader
                {


                    DocEntry = Convert.ToString(dt.Rows[i]["DocEntry"])
                    ,
                    DocNum = Convert.ToString(dt.Rows[i]["DocNum"])
                    ,

                    CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
                    ,
                    CardName = Convert.ToString(dt.Rows[i]["CardName"])
                    ,
                    DocDate = Convert.ToString(dt.Rows[i]["DocDate"])

                    ,
                    DocType = Convert.ToString(dt.Rows[i]["DocType"])
                    ,
                    CANCELED = Convert.ToString(dt.Rows[i]["CANCELED"])
                    ,
                    DocStatus = Convert.ToString(dt.Rows[i]["DocStatus"])
                    ,
                    DocTotal = Convert.ToString(dt.Rows[i]["DocTotal"])
                    ,
                    VatSum = Convert.ToString(dt.Rows[i]["VatSum"])
                    ,
                    DocDiscount = Convert.ToString(dt.Rows[i]["DiscSum"])
                    ,
                    MarketingDocument_Rows = GetInvoiceRows( DBName ,Convert.ToString(dt.Rows[i]["DocEntry"]))
                };
                invoices.Add(invoice);
            }
           
            var json = JsonConvert.SerializeObject(invoices, Newtonsoft.Json.Formatting.Indented);
            var response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content = new StringContent(json, Encoding.UTF8, "application/json");
            return response;
        }
        public List<MarketingDocument_Rows> GetInvoiceRows(string DBName,string DocEntry)
        {

            
          
            List<MarketingDocument_Rows> invoicerows = new List<MarketingDocument_Rows>();
            DataTable dt = null;
            bool is_Numeric = IsNumeric(DocEntry);
            if (is_Numeric == true)
            {


                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystring = "select  T1.\"DocEntry\" ,T1.\"ItemCode\", T1.\"ItemCode\", T1.\"Dscription\", T1.\"Quantity\", T1.\"WhsCode\", T1.\"LineTotal\", T1.\"PriceBefDi\",T1.\"Price\", T1.\"VatGroup\", T1.\"VatSum\" from  \""  + Sanitize(DBName) + "\" + \".INV1\" T1  INNER JOIN" +
                              "   \""  + Sanitize(DBName) + "\" + \".OINV\"  T0  ON  T0.\"DocEntry\" =T1.\"DocEntry\"  Where T0.\"DocEntry\" ='" + DocEntry + "'";
                }
                else
                {
                    querystring = "select  ISNULL(T1.DocEntry,'') 'DocEntry' , ISNULL(T1.ItemCode,'') 'ItemCode' , " +
                        " ISNULL(T1.ItemCode,'') 'ItemCode', ISNULL(T1.Dscription ,'') 'Dscription', ISNULL(T1.Quantity ,0) 'Quantity', ISNULL(T1.WhsCode ,'') 'WhsCode', " +
                        " ISNULL(T1.LineTotal,0) 'LineTotal',ISNULL( T1.PriceBefDi,0) 'PriceBefDi',ISNULL(T1.Price,0) 'Price'," +
                        " ISNULL(T1.VatGroup,'') 'VatGroup', ISNULL(T1.VatSum,0) 'VatSum' from "  + Sanitize(DBName) + ".[dbo]." + "INV1 T1 (nolock) INNER JOIN" +
                              " "  + Sanitize(DBName) + ".[dbo]." + "OINV  T0  ON  T0.DocEntry =T1.DocEntry  Where T0.DocEntry ='" + DocEntry + "'";
                }




                dt = GetData( querystring);
            }
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                invoicerows.Add(new MarketingDocument_Rows
                {


                    DocEntry = Convert.ToString(dt.Rows[i]["DocEntry"])
                     ,
                    ItemCode = Convert.ToString(dt.Rows[i]["ItemCode"])
                    ,
                    Dscription = Convert.ToString(dt.Rows[i]["Dscription"])
                    ,
                    Quantity = Convert.ToString(dt.Rows[i]["Quantity"])
                    ,
                    WhsCode = Convert.ToString(dt.Rows[i]["WhsCode"])
                    ,
                    LineTotal = Convert.ToString(dt.Rows[i]["LineTotal"])
                    ,
                    PriceBefDi = Convert.ToString(dt.Rows[i]["PriceBefDi"])
                      ,
                    Price = Convert.ToString(dt.Rows[i]["Price"])
                      ,
                    VatGroup = Convert.ToString(dt.Rows[i]["VatGroup"])
                    ,
                    VatSum = Convert.ToString(dt.Rows[i]["VatSum"])

                });
            }
            return invoicerows;
        }

        public class BusinessPartner_Master
        {
            public string CardCode { get; set; }
            public string CardName { get; set; }
            public double Balance { get; set; }
            public string Currency { get; set; }
            public double CreditLine { get; set; }
            public double DebtLine { get; set; }
            public List<BillToAddress> Bill_To_Address { get; set; }
        }

        public class BillToAddress
        {
            public string CardCode { get; set; }
            public string Name { get; set; }
            public string Title { get; set; }
            public string Position { get; set; }
            public string Address { get; set; }

            public string Tel1 { get; set; }

            public string Cellolar { get; set; }
            public string E_MailL { get; set; }

            public string Active { get; set; }
            // T1.[Name], T1.[Title], T1.[Position], T1.[Address], T1.[Tel1], T1.[Cellolar], T1.[E_MailL], T1.[Active]
        }
        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetBusinessPartner")]
        public HttpResponseMessage GetBusinessPartner(string DBName)
        {
            List<BusinessPartner_Master> customers = new List<BusinessPartner_Master>();


            if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                querystring = querystring = "select  T0.\"CardCode\", T0.\"CardName\", T0.\"Balance\", T0.\"CreditLine\", T0.\"DebtLine\", T0.\"Currency\" from  \""  + Sanitize(DBName) + "\" + \".OCRD\" T0  WHERE  T0.\"frozenFor\"='N'";

            }
            else
            {
                querystring = "select  isnull(T0.CardCode,'') 'CardCode', isnull(T0.CardName,'') 'CardName', isnull(T0.Balance,'0') 'Balance',isnull(T0.CreditLine,'0') 'CreditLine', isnull(T0.DebtLine,'0') 'DebtLine',isnull(T0.Currency,'') 'Currency' from  "  + Sanitize(DBName) + ".[dbo]." + "OCRD T0 (nolock) WHERE  frozenFor='N' ";
            }

            DataTable dt = GetData(querystring);
            for (int i = 0; i < dt.Rows.Count; i++)
            {

                BusinessPartner_Master customer = new BusinessPartner_Master
                {

                    CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
                    ,
                    CardName = Convert.ToString(dt.Rows[i]["CardName"])
                    ,
                    Balance = Convert.ToDouble(dt.Rows[i]["Balance"])
                    ,

                    CreditLine = Convert.ToDouble(dt.Rows[i]["CreditLine"]),

                    DebtLine = Convert.ToDouble(dt.Rows[i]["DebtLine"])
                    ,
                    Currency = Convert.ToString(dt.Rows[i]["Currency"])
                    ,
                    Bill_To_Address = GetContact(DBName,Convert.ToString(dt.Rows[i]["CardCode"]))
                };
                customers.Add(customer);
            }

            //  var json = new JavaScriptSerializer().Serialize(customers);
            var json = JsonConvert.SerializeObject(customers, Newtonsoft.Json.Formatting.Indented);
            var response = Request.CreateResponse(HttpStatusCode.OK);
            response.Content = new StringContent(json, Encoding.UTF8, "application/json");
            return response;
        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpPost]
        [Route("api/SAP/{DBName}/DeleteBusinessPartner/{SAPUserName}/{SAPPassword}")]
        //public HttpResponseMessage Post([FromBody]string value)
        //{
        public async Task<HttpResponseMessage> DeleteBusinessPartner(HttpRequestMessage request, string DBName, string SAPUserName, string SAPPassword)
        {
            //try
            //{
                var jsonString = await request.Content.ReadAsStringAsync();
                string bsl = @"\";
                JObject json = JObject.Parse(jsonString);
                var response = "";
                dynamic message_ = null;
                var message = "";
                string CardCode, CardName, CardType, Action;
                //Header Section 

                CardCode = (string)json.SelectToken("BPInformation").SelectToken("CardCode");
                CardName = (string)json.SelectToken("BPInformation").SelectToken("CardName");
                CardType = (string)json.SelectToken("BPInformation").SelectToken("CardType");
                Action = (string)json.SelectToken("BPInformation").SelectToken("Action");

                Connect_To_SAP connect = new Connect_To_SAP();
                oCompany = connect.ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                SAPbobsCOM.BusinessPartners sboBP = (SAPbobsCOM.BusinessPartners)oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oBusinessPartners);
                bool check_card_code = CheckIfExists(DBName, CardCode, CardType);
                if (check_card_code == true && Action == "DELETE")
                {
                    sboBP.GetByKey(CardCode);
                   
                                     

                    if (sboBP.Remove() != 0)
                    {
                        string dqt = @"""";
                        oCompany.GetLastError(out nErr, out erMsg);
                   
                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": " + dqt + erMsg.Replace("-","")+ dqt + ", \"Customer Number\": " + dqt + CardCode + dqt + ",\"Document Type\": \"Customer\"}}";
                    message_ = JsonConvert.DeserializeObject(message);

                    MarshallObject(sboBP);
                        MarshallObject(oCompany);
                    }
                    else
                    {

                      
                        message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Removed Business Partner\",\"Business Partner Number\": \"" + CardCode + "\",\"Document Type\": \"Business Partner\"}}";
                        message_ = JsonConvert.DeserializeObject(message);

                        SAP_SEND_MESSAGE("Business Partner Removed", "Business Partner Removed from API", "Business Partner Name", CardCode, "2", CardCode, "", "", "", "");

                        MarshallObject(sboBP);
                        MarshallObject(oCompany);


                    }

                }
                else
                {

                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"Customer Does not Exists \",\"Business Partner Number\": \"" + CardCode + "\" ,\"Document Type\": \"Business Partner\"}}";
                    message_ = JsonConvert.DeserializeObject(message);
                }
                var json_response = JsonConvert.SerializeObject(message_, Newtonsoft.Json.Formatting.Indented);
                var response_invoice = Request.CreateResponse(HttpStatusCode.OK);
                response_invoice.Content = new StringContent(json_response, Encoding.UTF8, "application/json");

                return response_invoice;


            //}
            //catch (Exception ex)
            //{

            //    HttpResponseMessage exeption_response = null;
            //    exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
            //    return exeption_response;
            //}




        }
        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpPost]
        [Route("api/SAP/{DBName}/CreateBusinessPartner/{SAPUserName}/{SAPPassword}")]
        //public HttpResponseMessage Post([FromBody]string value)
        //{
        public async Task<HttpResponseMessage> CreateBusinessPartner(HttpRequestMessage request ,string DBName, string SAPUserName, string SAPPassword)
        {
            try
            {
                var jsonString = await request.Content.ReadAsStringAsync();
                string bsl = @"\";
                JObject json = JObject.Parse(jsonString);
                var response = "";
                dynamic message_ = null;
                var message = "";
                string CardCode, CardName, CardType, GroupCode, Action, Telephone1, Telephone2, MobilePhone, KRAPIN, Email, Fax, AlternateCardCode, Uniqueid, CreateDate;
                //Header Section 

                CardCode = (string)json.SelectToken("BPInformation").SelectToken("CardCode");
                CardName = (string)json.SelectToken("BPInformation").SelectToken("CardName");
                CardType = (string)json.SelectToken("BPInformation").SelectToken("CardType");
                GroupCode = (string)json.SelectToken("BPInformation").SelectToken("GroupCode");
                Telephone1 = (string)json.SelectToken("BPInformation").SelectToken("Telephone1");
                Telephone2 = (string)json.SelectToken("BPInformation").SelectToken("Telephone2");
                Action = (string)json.SelectToken("BPInformation").SelectToken("Action");
                MobilePhone = (string)json.SelectToken("BPInformation").SelectToken("MobilePhone");
                KRAPIN = (string)json.SelectToken("BPInformation").SelectToken("KRAPIN");
                Email = (string)json.SelectToken("BPInformation").SelectToken("Email");
                Fax = (string)json.SelectToken("BPInformation").SelectToken("Fax");
                //Start  Header UDF  Declaration Section 
                AlternateCardCode = (string)json.SelectToken("BPInformation").SelectToken("AlternateCardCode");
                Uniqueid = (string)json.SelectToken("BPInformation").SelectToken("Uniqueid");
                CreateDate = (string)json.SelectToken("BPInformation").SelectToken("CreateDate");
                //End of Header UDF  Declaration Section 
                //Header Section 


                string S_AddressName1, S_AddressName2, S_POBox, S_Code, S_City;
                string B_AddressName1, B_AddressName2, B_POBox, B_Code, B_City;

                B_AddressName1 = (string)json.SelectToken("BilltoAdress").SelectToken("AddressName1");
                B_AddressName2 = (string)json.SelectToken("BilltoAdress").SelectToken("AddressName2");
                B_POBox = (string)json.SelectToken("BilltoAdress").SelectToken("POBox");
                B_Code = (string)json.SelectToken("BilltoAdress").SelectToken("Code");
                B_City = (string)json.SelectToken("BilltoAdress").SelectToken("City");

                S_AddressName1 = (string)json.SelectToken("ShiptoAdress").SelectToken("AddressName1");
                S_AddressName2 = (string)json.SelectToken("ShiptoAdress").SelectToken("AddressName2");
                S_POBox = (string)json.SelectToken("ShiptoAdress").SelectToken("POBox");
                S_Code = (string)json.SelectToken("ShiptoAdress").SelectToken("Code");
                S_City = (string)json.SelectToken("ShiptoAdress").SelectToken("City");

                //string BrokerCode, BrokerName, BrokerGroupCode;
                //BrokerCode = (string)json.SelectToken("Accounting").SelectToken("BrokerCode");
                //BrokerName = (string)json.SelectToken("Accounting").SelectToken("BrokerName");
                //BrokerGroupCode = (string)json.SelectToken("Accounting").SelectToken("BrokerGroupCode");

                Connect_To_SAP connect = new Connect_To_SAP();
                oCompany = connect.ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                SAPbobsCOM.BusinessPartners sboBP = (SAPbobsCOM.BusinessPartners)oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oBusinessPartners);
                bool check_card_code = CheckIfExists(DBName,CardCode, CardType);
                if (check_card_code == false && Action == "ADD")
                {

                    sboBP.CardCode = CardCode;
                    sboBP.CardName = CardName;
                    if (CardType == "CUSTOMER") 
                    { 
                        sboBP.CardType = BoCardTypes.cCustomer;
                    }
                    else if (CardType == "SUPLIER")
                    {
                        sboBP.CardType = BoCardTypes.cSupplier;
                    }
                    else if (CardType == "LEAD")
                    { sboBP.CardType = BoCardTypes.cLid; 
                    }
                    
                    sboBP.GroupCode = Convert.ToInt32(GroupCode);
                    sboBP.Phone1 = Telephone1;
                    sboBP.Phone2 = Telephone2;
                    sboBP.Cellular = MobilePhone;
                    sboBP.UnifiedFederalTaxID = KRAPIN;
                    sboBP.FederalTaxID = KRAPIN;
                    sboBP.EmailAddress = Email;
                    sboBP.Fax = Fax;

                    sboBP.Addresses.TypeOfAddress = Convert.ToString(BoAddressType.bo_BillTo);
                    sboBP.Addresses.AddressName = "Address1";
                    sboBP.Addresses.AddressName2 = S_AddressName2;
                    sboBP.Addresses.Street = S_POBox;
                    sboBP.Addresses.ZipCode = S_POBox;
                    sboBP.Addresses.City = S_City;
                    sboBP.Addresses.Add();

                    sboBP.SubjectToWithholdingTax = BoYesNoEnum.tYES;

                    //if (string.IsNullOrEmpty(BrokerCode))
                    //{
                    //    sboBP.WTCode = "WT05";
                    //}
                    //else
                    //{
                    //    sboBP.WTCode = "WT15";
                    //    sboBP.FatherCard = BrokerCode;
                    //    sboBP.FatherType = BoFatherCardTypes.cPayments_sum;
                    //}


                    if (sboBP.Add() != 0)
                    {
                        string dqt = @"""";
                        oCompany.GetLastError(out nErr, out erMsg);
                        message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": " + erMsg + ", \"Customer Number\": " + dqt + CardCode + dqt + ",\"Document Type\": \"Customer\"}}";
                        message_ = JsonConvert.DeserializeObject(message);

                        MarshallObject(sboBP);
                        MarshallObject(oCompany);
                    }
                    else
                    {
                        
                        //message = "{" + bsl + "Message" + bsl + ": {" + bsl + "MessageType" + bsl + ": " + bsl + "Success" + bsl + "," + bsl + "Description" + bsl + ": " + bsl + "Successfully Created  Customer " + bsl + "," + bsl + "Customer Number" + bsl + ": " + bsl + CardCode.ToString() + bsl + " ," + bsl + "Document Type" + bsl + ": " + bsl + "Customer" + bsl + "}}";
                        // message = message.Replace(bsl, dqt);
                        // var json_response = JsonConvert.SerializeObject(message, Newtonsoft.Json.Formatting.Indented);

                        message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created Customer\",\"Customer Number\": \"" + CardCode + "\",\"Document Type\": \"Customer\"}}";
                        message_ = JsonConvert.DeserializeObject(message);

                        SAP_SEND_MESSAGE("Customer Added", "Customer Added from API", "Customer Name", CardCode, "2", CardCode, "", "", "", "");

                        MarshallObject(sboBP);
                        MarshallObject(oCompany);


                    }

                }
                else {

                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"Customer Already Exists \",\"Customer Number\": \"" +CardCode + "\" ,\"Document Type\": \"Customer\"}}";
                    message_ = JsonConvert.DeserializeObject(message);
                }
                var json_response = JsonConvert.SerializeObject(message_, Newtonsoft.Json.Formatting.Indented);
                var response_invoice = Request.CreateResponse(HttpStatusCode.OK);
                response_invoice.Content = new StringContent(json_response, Encoding.UTF8, "application/json");

                return response_invoice;


            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }




        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpPost]
        [Route("api/SAP/{DBName}/CreateInvoice/{SAPUserName}/{SAPPassword}")]
        //public HttpResponseMessage Post([FromBody]string value)
        //{
        public async Task<HttpResponseMessage> CreateInvoice(HttpRequestMessage request, string DBName, string SAPUserName, string SAPPassword)
        {
            //try
            //{

                var jsonString = await request.Content.ReadAsStringAsync();
                //context.Response.ContentType = "text/JSON";
                string output = "";
                string message = "";
                dynamic message_ = null;

                JObject json = JObject.Parse(jsonString);

                // string SourceNumber = "";
                // string PaymentReference;

                string DocDate = "";
                string PostingDate = "";
                string CardCode = "";
                string CardName = "";
                string InvoiceType = "";
                string SourceNumber = "";
                string GroupCode = "";
                string Action = "";
                string Rounding = "";
                string Reference = "";
                string Remarks = "";


            AddUpdateAppSettings("CompanyDB", DBName);
            AddUpdateAppSettings("manager", SAPUserName);
            AddUpdateAppSettings("Password", SAPPassword);

            DocDate = (string)json.SelectToken("Header").SelectToken("DocDate");
                PostingDate = (string)json.SelectToken("Header").SelectToken("PostingDate");
                CardCode = (string)json.SelectToken("Header").SelectToken("CardCode");
                CardName = (string)json.SelectToken("Header").SelectToken("CardName");
                InvoiceType = (string)json.SelectToken("Header").SelectToken("InvoiceType");
                SourceNumber = (string)json.SelectToken("Header").SelectToken("SourceNumber");
                // GroupCode = (string) json.SelectToken("Header").SelectToken("GroupCode");
                Action = (string)json.SelectToken("Header").SelectToken("Action");
                Rounding = (string)json.SelectToken("Header").SelectToken("Rounding");
                Reference = (string)json.SelectToken("Header").SelectToken("Reference");
                Remarks = (string)json.SelectToken("Header").SelectToken("Remarks");
            oCompany = new Connect_To_SAP().ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                //int checkif_customer_exist = 0;



                //checkif_customer_exist = Check_If_Customer_Exists(DBName, CardCode,SAPUserName,  SAPPassword);
                //if (checkif_customer_exist == 0)
                //{

                //    Create_Customer(CardCode, CardName);

                //}



                // string Tagged_Get_FatherCard = Get_FatherCard(BrokerCode);

                //SAPbobsCOM.BusinessPartners sboBP = (SAPbobsCOM.BusinessPartners)oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oBusinessPartners);

                SAPbobsCOM.Documents oInvoice = null;

                oInvoice = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oInvoices);
                oInvoice.DocType = SAPbobsCOM.BoDocumentTypes.dDocument_Items;
                oInvoice.Rounding = BoYesNoEnum.tYES;
                oInvoice.DocDate = Convert.ToDateTime(DocDate);
                oInvoice.CardCode = CardCode;
                // oInvoice.SalesPersonCode = get_salespersoncode;
                oInvoice.NumAtCard = SourceNumber;
                //oInvoice.UserFields.Fields.Item("U_SourceNumber").Value = SourceNumber;
                oInvoice.Comments = Remarks + " " + "SourceNumber :" + SourceNumber;
                // oInvoice.BPL_IDAssignedToInvoice = Convert.ToInt32(Branch);

                JArray jarr = (JArray)json["Rows"];
                if (InvoiceType == "S")
                {
                    foreach (var item in jarr)
                    {
                        string Description, AcctCode, VatGroup, UnitPrice, LineTotal;

                        LineTotal = item.SelectToken("LineTotal").ToString();
                        Description = item.SelectToken("Description").ToString();
                        AcctCode = item.SelectToken("AcctCode").ToString();
                        UnitPrice = item.SelectToken("UnitPrice").ToString();
                        VatGroup = item.SelectToken("VatGroup").ToString();


                        oInvoice.Lines.ItemDescription = Description;
                        oInvoice.Lines.AccountCode = AcctCode;
                        oInvoice.Lines.VatGroup = VatGroup;
                        oInvoice.Lines.UnitPrice = Convert.ToDouble(UnitPrice);
                        oInvoice.Lines.LineTotal = Convert.ToDouble(LineTotal);
                        oInvoice.Lines.PriceAfterVAT = Convert.ToDouble(LineTotal);
                        oInvoice.Lines.Add();

                    }
                }
                else
                {

                    foreach (var item in jarr)
                    {
                        string ItemCode, Description, Quantity, UnitPrice, VatGroup, LineTotal, WarehouseCode;

                        ItemCode = item.SelectToken("ItemCode").ToString();
                        Description = item.SelectToken("Description").ToString();
                        Quantity = item.SelectToken("Quantity").ToString();
                        UnitPrice = item.SelectToken("UnitPrice").ToString();
                        VatGroup = item.SelectToken("VatGroup").ToString();
                        LineTotal = item.SelectToken("LineTotal").ToString();
                        WarehouseCode = item.SelectToken("WarehouseCode").ToString();


                        oInvoice.Lines.ItemCode = ItemCode;
                        oInvoice.Lines.Quantity = Convert.ToDouble(Quantity);
                        oInvoice.Lines.VatGroup = VatGroup;
                        oInvoice.Lines.UnitPrice = Convert.ToDouble(UnitPrice);
                        //oInvoice.Lines.LineTotal = Convert.ToDouble(LineTotal);
                        //oInvoice.Lines.PriceAfterVAT = Convert.ToDouble(LineTotal);
                        oInvoice.Lines.WarehouseCode = WarehouseCode;

                        oInvoice.Lines.Add();

                    }






                }
                if (oInvoice.Add() != 0)
                {

                    oCompany.GetLastError(out nErr, out erMsg);
                    
                    erMsg = Sanitize_Errors(erMsg);
                    //.Replace(dbqt, "");
                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Invoice\"}}";
                    message_ = JsonConvert.DeserializeObject(message);
                    MarshallObject(oInvoice);
                    MarshallObject(oCompany);
                }
                else
                {
                    //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                    // oInvoice.GetByKey(snum);
                    //XmlDocument doc = new XmlDocument();
                    //doc.LoadXml(oInvoice.GetAsXML());
                    //string jsonText = JsonConvert.SerializeXmlNode(doc);
                    //message = jsonText;
                    
                    string add_data =Get_InvoiceData(DBName ,CardCode);
                if (add_data.Length > 1)
                {

                    message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\"" + add_data;
                    // {\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"DocTotal\": \"3000\",\"VatSum\": \"0\",\"DiscSum\": \"0\"}}
                    //   message = message;
                    JObject json_data = JObject.Parse(message);
                    string DocEntry = json_data.SelectToken("Message").SelectToken("DocEntry").ToString();
                    message_ = JsonConvert.DeserializeObject(message);
                    SAP_SEND_MESSAGE("Invoice Added", "Invoice Added from API", "Customer Name", CardCode, "2", CardCode, "Invoice Number", DocEntry, "13", DocEntry);
                }
                else
                {
                    erMsg = "Customer Code  or ItemCode was not found in this  Comapany " + DBName;
                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Invoice\"}}";
                    message_ = JsonConvert.DeserializeObject(message);
                    MarshallObject(oInvoice);
                    MarshallObject(oCompany);
                }

                }
                var json_response = JsonConvert.SerializeObject(message_, Newtonsoft.Json.Formatting.None);
                var response_invoice = Request.CreateResponse(HttpStatusCode.OK);
                response_invoice.Content = new StringContent(json_response, Encoding.UTF8, "application/json");
                MarshallObject(oInvoice);
                MarshallObject(oCompany);
                MarshallObject(QueryObject);
                MarshallObject(QueryObjectDocEntry);
                return response_invoice;

            //}
            //catch (Exception ex)
            //{

            //    HttpResponseMessage exeption_response = null;
            //    exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
            //    return exeption_response;
            //}


        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpPost]
        [Route("api/SAP/{DBName}/CreateDocument/{SAPUserName}/{SAPPassword}")]
        //public HttpResponseMessage Post([FromBody]string value)
        //{
        public async Task<HttpResponseMessage> CreateDocument(HttpRequestMessage request, string DBName, string SAPUserName, string SAPPassword)
        {
            //try
            //{

            var jsonString = await request.Content.ReadAsStringAsync();
            //context.Response.ContentType = "text/JSON";
            string output = "";
            string message = "";
            dynamic message_ = null;

            JObject json = JObject.Parse(jsonString);

            // string SourceNumber = "";
            // string PaymentReference;

            string DocDate = "";
            string PostingDate = "";
            string DocDueDate = "";
            string RequiredDate = "";
            string CardCode = "";
            string CardName = "";
            string ObjectType = "";
            string DocType = "";
            string SourceNumber = "";
            string GroupCode = "";
            string Action = "";
            string Rounding = "";
            string Reference = "";
            string Remarks = "";


            AddUpdateAppSettings("CompanyDB", DBName);
            AddUpdateAppSettings("manager", SAPUserName);
            AddUpdateAppSettings("Password", SAPPassword);

            DocDate = (string)json.SelectToken("Header").SelectToken("DocDate");
            PostingDate = (string)json.SelectToken("Header").SelectToken("PostingDate");
            DocDueDate = (string)json.SelectToken("Header").SelectToken("DocDueDate");
            RequiredDate = (string)json.SelectToken("Header").SelectToken("RequiredDate");
            ObjectType = (string)json.SelectToken("Header").SelectToken("ObjectType");
            CardCode = (string)json.SelectToken("Header").SelectToken("CardCode");
            CardName = (string)json.SelectToken("Header").SelectToken("CardName");
            DocType = (string)json.SelectToken("Header").SelectToken("DocType");
            SourceNumber = (string)json.SelectToken("Header").SelectToken("SourceNumber");
            Action = (string)json.SelectToken("Header").SelectToken("Action");
            Rounding = (string)json.SelectToken("Header").SelectToken("Rounding");
            Reference = (string)json.SelectToken("Header").SelectToken("Reference");
            Remarks = (string)json.SelectToken("Header").SelectToken("Remarks");
            oCompany = new Connect_To_SAP().ConnectSAPDB(DBName, SAPUserName, SAPPassword);
            
            string Table = "";

            SAPbobsCOM.Documents oDoc = null;

            string ObjectTypeNum = "";
            if (ObjectType == "SALESQOUTE")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oQuotations);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "OQUT";
                ObjectTypeNum = "23";
            }
            else if (ObjectType == "SALESORDER")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oOrders);
                oDoc.CardCode = CardCode;
                oDoc.DocDueDate = Convert.ToDateTime(DocDueDate);
                oDoc.NumAtCard = SourceNumber;
                Table = "ORDR";
                ObjectTypeNum = "17";
            }
            else if (ObjectType == "SALESCREDITNOTE")
            {

                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oCreditNotes);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "ORIN";
                ObjectTypeNum = "14";
            }
            else if (ObjectType == "SALESINVOICE")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oInvoices);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "OINV";
                ObjectTypeNum = "13";

            }
            else if (ObjectType == "PURCHASEREQUEST")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseRequest);
                oDoc.RequriedDate = Convert.ToDateTime(RequiredDate);
                
                Table = "OPRQ";
                ObjectTypeNum = "1470000113";
            }
            else if (ObjectType == "PURCHASEQOUTATION")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseQuotations);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "OPQT";
                ObjectTypeNum = "540000006";
                oDoc.RequriedDate = Convert.ToDateTime(RequiredDate);
            }
            else if (ObjectType == "PURCHASEORDER")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseOrders);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "OPOR";
                ObjectTypeNum = "22";
            }
            else if (ObjectType == "PURCHASECREDITNOTE")
            {

                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseCreditNotes);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "ORPC";
                ObjectTypeNum = "19";
            }
            else if (ObjectType == "GRPO")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseDeliveryNotes);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "OPDN";
                ObjectTypeNum = "20";
            }
            else if (ObjectType == "GOODSRETURN")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseReturns);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "ORPD";
                ObjectTypeNum = "21";
            }
            else if (ObjectType == "PURCHASEINVOICE")
            {
                oDoc = (Documents)oCompany.GetBusinessObject(BoObjectTypes.oPurchaseInvoices);
                oDoc.CardCode = CardCode;
                oDoc.NumAtCard = SourceNumber;
                Table = "OPCH";
                ObjectTypeNum = "18";
            }

          
            oDoc.Rounding = BoYesNoEnum.tYES;        
            
            oDoc.TaxDate = Convert.ToDateTime(PostingDate);
            oDoc.DocDate = Convert.ToDateTime(DocDate);
            // oDoc.SalesPersonCode = get_salespersoncode;
            
            //oDoc.UserFields.Fields.Item("U_SourceNumber").Value = SourceNumber;
            oDoc.Comments = Remarks + " " + "SourceNumber :" + SourceNumber;
            // oDoc.BPL_IDAssignedToDoc = Convert.ToInt32(Branch);

            JArray jarr = (JArray)json["Rows"];
            if (DocType == "S")
            {
                foreach (var item in jarr)
                {
                    string Description, AcctCode, VatGroup, UnitPrice, LineTotal;

                    LineTotal = item.SelectToken("LineTotal").ToString();
                    Description = item.SelectToken("Description").ToString();
                    AcctCode = item.SelectToken("AcctCode").ToString();
                    UnitPrice = item.SelectToken("UnitPrice").ToString();
                    VatGroup = item.SelectToken("VatGroup").ToString();

                    oDoc.DocType = SAPbobsCOM.BoDocumentTypes.dDocument_Service;
                    oDoc.Lines.ItemDescription = Description;
                    oDoc.Lines.AccountCode = AcctCode;
                    oDoc.Lines.VatGroup = VatGroup;
                    oDoc.Lines.UnitPrice = Convert.ToDouble(UnitPrice);
                    oDoc.Lines.LineTotal = Convert.ToDouble(LineTotal);
                    oDoc.Lines.PriceAfterVAT = Convert.ToDouble(LineTotal);
                    oDoc.Lines.Add();

                }
            }
            else
            {

                foreach (var item in jarr)
                {
                    string ItemCode, Description, Quantity, UnitPrice, VatGroup, LineTotal, WarehouseCode;

                    ItemCode = item.SelectToken("ItemCode").ToString();
                    Description = item.SelectToken("Description").ToString();
                    Quantity = item.SelectToken("Quantity").ToString();
                    UnitPrice = item.SelectToken("UnitPrice").ToString();
                    VatGroup = item.SelectToken("VatGroup").ToString();
                    LineTotal = item.SelectToken("LineTotal").ToString();
                    WarehouseCode = item.SelectToken("WarehouseCode").ToString();

                    oDoc.DocType = SAPbobsCOM.BoDocumentTypes.dDocument_Items;
                    oDoc.Lines.ItemCode = ItemCode;
                    oDoc.Lines.Quantity = Convert.ToDouble(Quantity);
                    oDoc.Lines.VatGroup = VatGroup;
                    oDoc.Lines.UnitPrice = Convert.ToDouble(UnitPrice);
                    //oDoc.Lines.LineTotal = Convert.ToDouble(LineTotal);
                    //oDoc.Lines.PriceAfterVAT = Convert.ToDouble(LineTotal);
                    oDoc.Lines.WarehouseCode = WarehouseCode;

                    oDoc.Lines.Add();

                }






            }
            if (oDoc.Add() != 0)
            {

                oCompany.GetLastError(out nErr, out erMsg);

                erMsg = Sanitize_Errors(erMsg);
                //.Replace(dbqt, "");
                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Invoice\"}}";
                message_ = JsonConvert.DeserializeObject(message);
                MarshallObject(oDoc);
                MarshallObject(oCompany);
            }
            else
            {
                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                // oDoc.GetByKey(snum);
                //XmlDocument doc = new XmlDocument();
                //doc.LoadXml(oDoc.GetAsXML());
                //string jsonText = JsonConvert.SerializeXmlNode(doc);
                //message = jsonText;

                string add_data = Get_DocData(DBName,Table, CardCode);
                if (add_data.Length > 1)
                {

                    message = "{\"Message\": {\"MessageType\": \"Success\",\"ObjectType\": \"" + ObjectType + "\",\"Description\": \"Successfully Created\"" + add_data;
                   
                    JObject json_data = JObject.Parse(message);
                    string DocEntry = json_data.SelectToken("Message").SelectToken("DocEntry").ToString();
                    message_ = JsonConvert.DeserializeObject(message);
                    SAP_SEND_MESSAGE("Document  Added", "Document Added from API", "Business Partner Name", CardCode, "2", CardCode, "Document Number", DocEntry,ObjectTypeNum, DocEntry);
                }
                else
                {
                    erMsg = "Customer Code  or ItemCode was not found in this  Comapany " + DBName;
                    message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Invoice\"}}";
                    message_ = JsonConvert.DeserializeObject(message);
                    MarshallObject(oDoc);
                    MarshallObject(oCompany);
                }

            }
            var json_response = JsonConvert.SerializeObject(message_, Newtonsoft.Json.Formatting.None);
            var response_invoice = Request.CreateResponse(HttpStatusCode.OK);
            response_invoice.Content = new StringContent(json_response, Encoding.UTF8, "application/json");

            MarshallObject(QueryObjectDocEntry);
            MarshallObject(QueryObject);           
            MarshallObject(oDoc);
            MarshallObject(oCompany);
            return response_invoice;

            //}
            //catch (Exception ex)
            //{

            //    HttpResponseMessage exeption_response = null;
            //    exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
            //    return exeption_response;
            //}


        }


        [Authorize(Roles = "SuperAdmin, Admin, User")]
        [HttpPost]
        [Route("api/SAP/{DBName}/CreatePayment/{SAPUserName}/{SAPPassword}")]

        //public HttpResponseMessage Post([FromBody]string value)
        //{
        public async Task<HttpResponseMessage> PostPayment(HttpRequestMessage request, string DBName, string SAPUserName, string SAPPassword)
        {
            try
            {
                string message = "";
                dynamic message_ = null;
                var jsonString = await request.Content.ReadAsStringAsync();
                JObject json = JObject.Parse(jsonString);


                string DocDate = "";
                string PostingDate = "";
                string CardCode = "";
                string Action = "";
                string Rounding = "";
                string SourceNumber = "";
                string InvoiceDocEntry = "";
                string ReceiptNo = "";
                int DocEntry = 0;
               // string SumApplied = "";
                DocDate = (string)json.SelectToken("Header").SelectToken("DocDate");
                PostingDate = (string)json.SelectToken("Header").SelectToken("PostingDate");
                CardCode = (string)json.SelectToken("Header").SelectToken("CardCode");
                Action = (string)json.SelectToken("Header").SelectToken("Action");
                Rounding = (string)json.SelectToken("Header").SelectToken("Rounding");
                SourceNumber = (string)json.SelectToken("Header").SelectToken("SourceNumber");
                InvoiceDocEntry = (string)json.SelectToken("Header").SelectToken("InvoiceDocEntry");
                ReceiptNo = (string)json.SelectToken("Header").SelectToken("ReceiptNo");
                //SumApplied = (string)json.SelectToken("Header").SelectToken("SumApplied");




                Connect_To_SAP connect = new Connect_To_SAP();
                oCompany = connect.ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                SAPbobsCOM.Payments oPayment = (SAPbobsCOM.Payments)oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oIncomingPayments);

                if (string.IsNullOrEmpty(InvoiceDocEntry))
                {
                    DocEntry = Get_DocEntry( DBName,SourceNumber, CardCode);
                }
                else
                {
                    DocEntry = Convert.ToInt32(InvoiceDocEntry);
                }




                JArray jPayments = (JArray)json["Payments"];
                foreach (var Payment_item in jPayments)
                {
                    string PaymentDate, PaymentReference, PaymentType, Account, Amount;

                    PaymentDate = Payment_item.SelectToken("PaymentDate").ToString();
                    PaymentReference = Payment_item.SelectToken("PaymentReference").ToString();
                    PaymentType = Payment_item.SelectToken("PaymentType").ToString().ToUpper();
                    Account = Payment_item.SelectToken("Account").ToString();
                    Amount = Payment_item.SelectToken("Amount").ToString();

                    if (!string.IsNullOrEmpty(CardCode))
                    {


                        oPayment.DocDate = Convert.ToDateTime(DocDate);

                        oPayment.DocObjectCode = SAPbobsCOM.BoPaymentsObjectType.bopot_IncomingPayments;
                       // oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                        // oPayment.BPLID = Branchid;


                        if (PaymentType == "CHEQUE")
                        {

                            if (DocEntry != 0)
                            {
                                oPayment.DocType = SAPbobsCOM.BoRcptTypes.rCustomer;
                                oPayment.Invoices.DocEntry = DocEntry;
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.CardCode = CardCode;
                                //oPayment.BPLID = Branchid;
                                oPayment.Checks.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.Checks.CheckNumber = Convert.ToInt32(PaymentReference);
                                oPayment.Checks.CheckSum = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;
                                // oPayment.Checks.CountryCode = "KE";
                                oPayment.Checks.ManualCheck = SAPbobsCOM.BoYesNoEnum.tNO;
                                oPayment.Checks.Trnsfrable = SAPbobsCOM.BoYesNoEnum.tNO;

                                // oPayment.Checks.BankCode = "03";
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;
                            }
                            else
                            { 
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.CardCode = CardCode;
                                //oPayment.BPLID = Branchid;
                                oPayment.Checks.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.Checks.CheckNumber = Convert.ToInt32(PaymentReference);
                                oPayment.Checks.CheckSum = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;
                                //   oPayment.Checks.CountryCode = "KE";
                                oPayment.Checks.ManualCheck = SAPbobsCOM.BoYesNoEnum.tNO;
                                oPayment.Checks.Trnsfrable = SAPbobsCOM.BoYesNoEnum.tNO;
                                // // oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                //  oPayment.Checks.BankCode = "03";
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }


                            if (oPayment.Add() != 0)
                            {

                                oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                            }
                            else
                            {
                                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                                //oPayment.GetByKey(snum);
                                // int DocNum = oPayment.DocNum;
                                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                                // SAP_SEND_MESSAGE("Payment Added", "Payment Added from API", "Customer Name", CardCode, "2", CardCode, "Payment Number", snum.ToString(), "24", snum.ToString());
                            }
                        }

                        else if (PaymentType == "MOBILEPAYMENT")
                        {
                            oPayment.DocType = SAPbobsCOM.BoRcptTypes.rCustomer;
                            if (DocEntry != 0)
                            {

                                oPayment.Invoices.DocEntry = DocEntry;
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TransferAccount = Account;
                                oPayment.CardCode = CardCode;
                                // oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.TransferReference = (PaymentReference);
                                oPayment.TransferSum = Convert.ToDouble(Amount);
                                oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            else
                            {
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TransferAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.TransferReference = (PaymentReference);
                                oPayment.TransferSum = Convert.ToDouble(Amount);
                            oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            if (oPayment.Add() != 0)
                            {

                                oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                            }
                            else
                            {
                                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                                //int DocNum = oPayment.DocNum;
                                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                                // SAP_SEND_MESSAGE("Payment Added", "Payment Added from API", "Customer Name", CardCode, "2", CardCode, "Payment Number", snum.ToString(), "24", snum.ToString());
                            }
                        }

                        else if (PaymentType == "CASH")
                        {
                            oPayment.DocType = SAPbobsCOM.BoRcptTypes.rCustomer;
                            if (DocEntry != 0)
                            {

                                oPayment.Invoices.DocEntry = DocEntry;
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.CashAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.Remarks = PaymentReference;
                                oPayment.CashSum = Convert.ToDouble(Amount);
                                 oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            else
                            {
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.CashAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.Remarks = PaymentReference;
                                oPayment.CashSum = Convert.ToDouble(Amount);
                               oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            if (oPayment.Add() != 0)
                            {

                                oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                            }
                            else
                            {
                                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                                //int DocNum = oPayment.DocNum;
                                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                                // SAP_SEND_MESSAGE("Payment Added", "Payment Added from API", "Customer Name", CardCode, "2", CardCode, "Payment Number", snum.ToString(), "24", snum.ToString());
                            }
                        }
                        else if (PaymentType == "BANKTRANSFER")
                        {
                            oPayment.DocType = SAPbobsCOM.BoRcptTypes.rCustomer;
                            if (DocEntry != 0)
                            {

                                oPayment.Invoices.DocEntry = DocEntry;
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TransferAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.TransferReference = (PaymentReference);
                                oPayment.TransferSum = Convert.ToDouble(Amount);
                                oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;
                            }
                            else
                            {
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TransferAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.TransferReference = (PaymentReference);
                                oPayment.TransferSum = Convert.ToDouble(Amount);
                            oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            if (oPayment.Add() != 0)
                            {

                                oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                            }
                            else
                            {
                                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                                //int DocNum = oPayment.DocNum;
                                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                                // SAP_SEND_MESSAGE("Payment Added", "Payment Added from API", "Customer Name", CardCode, "2", CardCode, "Payment Number", snum.ToString(), "24", snum.ToString());
                            }
                        }

                        else if (PaymentType == "CREDITCARD")
                        {
                            oPayment.DocType = SAPbobsCOM.BoRcptTypes.rCustomer;
                            if (DocEntry != 0)
                            {

                                oPayment.Invoices.DocEntry = DocEntry;
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.CreditCards.CreditSum = Convert.ToDouble(Amount);
                                //DateTime cr_year = DateTime.Today.ToString("yyyy-MM-dd"));
                                //DateTime cr_month = (DateTime.Today.ToString("yyyy-MM-dd"));
                                oPayment.CreditCards.CardValidUntil = DateTime.Now;
                                oPayment.CreditCards.CreditAcct = Account;
                                oPayment.CreditCards.CreditCard = Get_CreditCard( DBName,Account);
                                oPayment.CreditCards.CreditCardNumber = PaymentReference;
                                oPayment.CreditCards.VoucherNum = PaymentReference;
                                oPayment.CreditCards.FirstPaymentDue = Convert.ToDateTime(PaymentDate);
                                oPayment.CreditCards.NumOfPayments = 1;
                                 oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;


                            }
                            else
                            {
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.CreditCards.CreditSum = Convert.ToDouble(Amount);
                                //DateTime cr_year = DateTime.Today.ToString("yyyy-MM-dd"));
                                //DateTime cr_month = (DateTime.Today.ToString("yyyy-MM-dd"));
                                oPayment.CreditCards.CardValidUntil = DateTime.Now;
                                oPayment.CreditCards.CreditAcct = Account;
                                oPayment.CreditCards.CreditCardNumber = PaymentReference;
                                oPayment.CreditCards.VoucherNum = PaymentReference;
                                oPayment.CreditCards.FirstPaymentDue = Convert.ToDateTime(PaymentDate);
                                oPayment.CreditCards.NumOfPayments = 1;
                                 oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            if (oPayment.Add() != 0)
                            {

                                oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                            }
                            else
                            {
                                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                                //int DocNum = oPayment.DocNum;
                                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"Document Type\": \"Payment\"}}";
                                message_ = JsonConvert.DeserializeObject(message);
                                // SAP_SEND_MESSAGE("Payment Added", "Payment Added from API", "Customer Name", CardCode, "2", CardCode, "Payment Number", snum.ToString(), "24", snum.ToString());
                            }
                        }


                        else if (PaymentType == "OTHER")
                        {
                            oPayment.DocType = SAPbobsCOM.BoRcptTypes.rCustomer;
                            if (DocEntry != 0)
                            {

                                oPayment.Invoices.DocEntry = DocEntry;
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TransferAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.TransferReference = (PaymentReference);
                                oPayment.TransferSum = Convert.ToDouble(Amount);
                              oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;
                            }
                            else
                            {
                                oPayment.DocDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TaxDate = Convert.ToDateTime(PaymentDate);
                                oPayment.DueDate = Convert.ToDateTime(PaymentDate);
                                oPayment.TransferAccount = Account;
                                oPayment.CardCode = CardCode;
                                //   oPayment.BPLID = Branchid;
                                oPayment.TransferDate = Convert.ToDateTime(PaymentDate);
                                oPayment.Remarks = PaymentReference;
                                oPayment.TransferReference = (PaymentReference);
                                oPayment.TransferSum = Convert.ToDouble(Amount);
                                oPayment.Invoices.SumApplied = Convert.ToDouble(Amount);
                                oPayment.ApplyVAT = BoYesNoEnum.tNO;

                            }
                            if (oPayment.Add() != 0)
                            {

                                oCompany.GetLastError(out nErr, out erMsg); erMsg = Sanitize_Errors(erMsg);
                                message = "{\"Message\": {\"MessageType\": \"Error\",\"Description\": \"" + erMsg + "\",\"Source Number\": \"" + SourceNumber + "\",\"Destination Number\": \"0\",\"Document Type\": \"Payment\"}}";
                                 message_ = JsonConvert.DeserializeObject(message);
                            }
                            else
                            {
                                //int snum = Int32.Parse(oCompany.GetNewObjectKey());
                                //int DocNum = oPayment.DocNum;
                                message = "{\"Message\": {\"MessageType\": \"Success\",\"Description\": \"Successfully Created\",\"Document Type\": \"Payment\"}}";
                                //string str = "{\"objects\":[{\"id\":1,\"title\":\"Book\",\"position_x\":0,\"position_y\":0,\"position_z\":0,\"rotation_x\":0,\"rotation_y\":0,\"rotation_z\":0,\"created\":\"2016-09-21T14:22:22.817Z\"},{\"id\":2,\"title\":\"Apple\",\"position_x\":0,\"position_y\":0,\"position_z\":0,\"rotation_x\":0,\"rotation_y\":0,\"rotation_z\":0,\"created\":\"2016-09-21T14:22:52.368Z\"}]}";
                                 message_ = JsonConvert.DeserializeObject(message);
                                // SAP_SEND_MESSAGE("Payment Added", "Payment Added from API", "Customer Name", CardCode, "2", CardCode, "Payment Number", snum.ToString(), "24", snum.ToString());
                            }
                        }

                       

                    }
                }
                var json_response = JsonConvert.SerializeObject(message_, Newtonsoft.Json.Formatting.Indented);
                var response_invoice = Request.CreateResponse(HttpStatusCode.OK);
                response_invoice.Content = new StringContent(json_response, Encoding.UTF8, "application/json");
                MarshallObject(oPayment);
                MarshallObject(oCompany);
                return response_invoice;
            }

            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }
        }

        public string Get_InvoiceData(string DBName ,string CardCode)
        {

            {
                string results = "";
                string DocTotal = "";
                string VatSum = "";
                string DiscSum = "";
                 QueryObject = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);
                 QueryObjectDocEntry = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);

                string querystringDocEntry = "";
                string DocEntry = "";
                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystringDocEntry = "SELECT  Top 1 \"DocEntry\"  FROM \"OINV\"   ORDER BY  DocEntry DESC";
                    QueryObjectDocEntry.DoQuery(querystringDocEntry);
                    DocEntry = (QueryObjectDocEntry.Fields.Item(0).Value.ToString());
                    querystring = "SELECT   \"DocTotal\" ,\"VatSum\" ,\"DiscSum\"  FROM \"OINV\"   WHERE \"DocEntry\" = '" + DocEntry+ "'";
                   

                }
                else
                {
                    querystringDocEntry = "SELECT  Top 1 DocEntry  FROM   "  + Sanitize(DBName) + ".[dbo]." + "OINV   (nolock) where CardCode ='"+ Sanitize(CardCode) +"'  ORDER BY  DocEntry DESC";
                    QueryObjectDocEntry.DoQuery(querystringDocEntry);
                    DocEntry = (QueryObjectDocEntry.Fields.Item(0).Value.ToString());
                    querystring = "SELECT   DocTotal ,VatSum,DiscSum FROM "  + Sanitize(DBName) + ".[dbo]." + "OINV  (nolock) WHERE DocEntry = '" + DocEntry + "' and CardCode ='" + Sanitize(CardCode) + "'";

                }


                QueryObject.DoQuery(querystring);
                DocTotal = (QueryObject.Fields.Item(0).Value.ToString());
                VatSum = (QueryObject.Fields.Item(1).Value.ToString());
                DiscSum = (QueryObject.Fields.Item(2).Value.ToString());


                results = ",\"DocEntry\": \"" + DocEntry + "\",\"DocTotal\": \"" + DocTotal + "\",\"VatSum\": \"" + VatSum + "\",\"DiscSum\": \"" + DiscSum + "\"}}";

                //results = @"\""DocTotal\"": \""results1\""""
                return results;

            }
        }


        public string Get_DocData(string DBName, string Table, string CardCode)
        {

            {
                string results = "";
                string DocTotal = "";
                string VatSum = "";
                string DiscSum = "";
                QueryObject = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);
                QueryObjectDocEntry = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);

                string querystringDocEntry = "";
                string DocEntry = "";
                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystringDocEntry = "SELECT  Top 1 \"DocEntry\"  FROM \"+ Table +\"   ORDER BY  DocEntry DESC";
                    QueryObjectDocEntry.DoQuery(querystringDocEntry);
                    DocEntry = (QueryObjectDocEntry.Fields.Item(0).Value.ToString());
                    querystring = "SELECT   \"DocTotal\" ,\"VatSum\" ,\"DiscSum\"  FROM \"OINV\"   WHERE \"DocEntry\" = '" + DocEntry + "'";


                }
                else
                {
                    querystringDocEntry = "SELECT  Top 1 DocEntry  FROM   "  + Sanitize(DBName) + ".[dbo]. "  + Table + "   (nolock) where CardCode ='" + Sanitize(CardCode) + "'  ORDER BY  DocEntry DESC";
                    QueryObjectDocEntry.DoQuery(querystringDocEntry);
                    DocEntry = (QueryObjectDocEntry.Fields.Item(0).Value.ToString());
                    querystring = "SELECT   DocTotal ,VatSum,DiscSum FROM "  + Sanitize(DBName) + ".[dbo]. "  + Table + "(nolock) WHERE DocEntry = '" + DocEntry + "' and CardCode ='" + Sanitize(CardCode) + "'";

                }


                QueryObject.DoQuery(querystring);
                DocTotal = (QueryObject.Fields.Item(0).Value.ToString());
                VatSum = (QueryObject.Fields.Item(1).Value.ToString());
                DiscSum = (QueryObject.Fields.Item(2).Value.ToString());


                results = ",\"DocEntry\": \"" + DocEntry + "\",\"DocTotal\": \"" + DocTotal + "\",\"VatSum\": \"" + VatSum + "\",\"DiscSum\": \"" + DiscSum + "\"}}";

                //results = @"\""DocTotal\"": \""results1\""""
                return results;

            }
        }

        public int Get_CreditCard(string DBName,string Account)
        {

            {
                int results = 1;
                Recordset QueryObject = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);

                //string querystring = "SELECT   DocEntry FROM OINV  (nolock) WHERE NumAtCard = '" + Sanitize(InvoiceReference) + "'  and  CardCode ='" + Sanitize(CardCode) + "'";

                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystring = "SELECT   \"CreditCard\" FROM  \""  + Sanitize(DBName) + "\" + \".OCRC\"   WHERE \"AcctCode\" = '" + (Account) + "'";

                }
                else
                {
                    querystring = "SELECT   CreditCard FROM  "  + Sanitize(DBName) + ".[dbo]." + "OCRC  (nolock) WHERE AcctCode = '" + (Account) + "'";

                }


                QueryObject.DoQuery(querystring);
                results = Convert.ToInt32(QueryObject.Fields.Item(0).Value.ToString());
                if (QueryObject != null)
                {
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(QueryObject);
                    QueryObject = null;
                }
                return results;

            }
        }
            public int Get_DocEntry(string DBName,string InvoiceReference, string CardCode)
        {

            {
                int results = 0;
                Recordset QueryObject = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);

                //string querystring = "SELECT   DocEntry FROM OINV  (nolock) WHERE NumAtCard = '" + Sanitize(InvoiceReference) + "'  and  CardCode ='" + Sanitize(CardCode) + "'";

                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystring = "SELECT TOP 1   \"DocEntry\" FROM  \""  + Sanitize(DBName) + "\" + \".OINV\"   WHERE \"NumAtCard\" = '" + Sanitize(InvoiceReference) + "'  and  \"CardCode\" ='" + Sanitize(CardCode) + "'";

                }
                else
                {
                    querystring = "SELECT TOP 1  DocEntry FROM  "  + Sanitize(DBName) + ".[dbo]." + "OINV  (nolock) WHERE NumAtCard = '" + Sanitize(InvoiceReference) + "'  and  CardCode ='" + Sanitize(CardCode) + "'";

                }


                QueryObject.DoQuery(querystring);
                results = Convert.ToInt32(QueryObject.Fields.Item(0).Value.ToString());
                if (QueryObject != null)
                {
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(QueryObject);
                    QueryObject = null;
                }
                return results;

            }
        }
        public void MarshallObject(object Object)
        {
            try
            {
                if (Object != null)
                {
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(Object);
                    Object = null;
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
            catch (Exception ex)
            {

            }

        }

        public void SAP_SEND_MESSAGE(string subject, string message , string ColumnOneName ,string ColumnOneValue, string ColumnOneObject, string ColumnOneObjectKey, string ColumnTwoName, string ColumnTwoValue, string ColumnTwoObject, string ColumnTwoObjectKey)
        {
            try
            {
                if (MessageFlag == "ON")
                {

                    SAPbobsCOM.CompanyService oCmpSrv;
                    MessagesService oMessageService;

                    //get company service
                    oCmpSrv = oCompany.GetCompanyService();

                    oMessageService = (SAPbobsCOM.MessagesService)oCmpSrv.GetBusinessService(ServiceTypes.MessagesService);

                    SAPbobsCOM.Message oMessage = null;
                    MessageDataColumns pMessageDataColumns = null;
                    MessageDataColumn pMessageDataColumn = null;
                    MessageDataLines oLines = null;
                    MessageDataLine oLine = null;
                    RecipientCollection oRecipientCollection = null;

                    // get the data interface for the new message
                    oMessage = ((SAPbobsCOM.Message)(oMessageService.GetDataInterface(MessagesServiceDataInterfaces.msdiMessage)));

                    // fill subject
                    oMessage.Subject = subject;
                    oMessage.Text = message;

                    // Add Recipient 
                    oRecipientCollection = oMessage.RecipientCollection;

                    // se agregan dos usuarios a los cuales les llegara el mensaje/alerta
                    oRecipientCollection.Add();
                    oRecipientCollection.Item(0).SendInternal = BoYesNoEnum.tYES; // send internal message
                    oRecipientCollection.Item(0).UserCode = "manager"; // add existing user name
                                                                       //oRecipientCollection.Add();
                                                                       //oRecipientCollection.Item(1).SendInternal = BoYesNoEnum.tYES; // send internal message
                                                                       //oRecipientCollection.Item(1).UserCode = "ventas"; // add existing user name

                    // agregamos nuestro listado de documento o documentos que se mostrara en el mensaje/alertas
                    pMessageDataColumns = oMessage.MessageDataColumns; // get columns data

                    if ((ColumnOneName.Length > 1) && (ColumnTwoName.Length > 1))
                    {
                        pMessageDataColumn = pMessageDataColumns.Add(); // get column
                        pMessageDataColumn.ColumnName = ColumnOneName; // set column name
                        pMessageDataColumn.Link = BoYesNoEnum.tYES; // set link to a real object in the application
                                                                    // agregamos las partidas

                        oLines = pMessageDataColumn.MessageDataLines; // get lines
                        oLine = oLines.Add(); // add new line
                        oLine.Value = ColumnOneValue; // set the line value
                        oLine.Object = ColumnOneObject; // set the link to object Document 
                        oLine.ObjectKey = ColumnOneObjectKey; // set the Document code (DocEntry)

                        pMessageDataColumn = pMessageDataColumns.Add(); // get column
                        pMessageDataColumn.ColumnName = ColumnTwoName; // set column name
                        pMessageDataColumn.Link = BoYesNoEnum.tYES; // set link to a real object in the application
                                                                    // agregamos las partidas
                        oLines = pMessageDataColumn.MessageDataLines; // get lines
                        oLine = oLines.Add(); // add new line
                        oLine.Value = ColumnTwoValue; // set the line value
                        oLine.Object = ColumnTwoObject; // set the link to object Document 
                        oLine.ObjectKey = ColumnTwoObjectKey; // set the Document code (DocEntry)
                    }
                    else
                    {

                        pMessageDataColumn = pMessageDataColumns.Add(); // get column
                        pMessageDataColumn.ColumnName = ColumnOneName; // set column name
                        pMessageDataColumn.Link = BoYesNoEnum.tYES; // set link to a real object in the application
                                                                    // agregamos las partidas
                        oLines = pMessageDataColumn.MessageDataLines; // get lines
                        oLine = oLines.Add(); // add new line
                        oLine.Value = ColumnOneValue; // set the line value
                        oLine.Object = ColumnOneObject; // set the link to object Document 
                        oLine.ObjectKey = ColumnOneObjectKey; // set the Document code (DocEntry)
                    }
                    // send the message
                    //oMessage.Add();
                    oMessageService.SendMessage(oMessage);
                    //resul = true;

                    GC.Collect();
                    //return resul;
                }  
            }
            catch (Exception ex)
            {
                //_message_error += "warning: " + ex.Message;
               // return false;
            }
            
        }
        public void SendSAPMessage(string subject , string message)
        {

            SAPbobsCOM.Messages oMsg = oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oMessages);
            try
            {

                //Connect_To_SAP connect = new Connect_To_SAP();
                //oCompany = connect.ConnectSAPDB();
                oMsg.Subject = subject;
                oMsg.MessageText = message;
                oMsg.Recipients.SetCurrentLine(0);
                oMsg.Recipients.UserCode = "manager";
                oMsg.Recipients.NameTo = "manager";
                oMsg.Recipients.SendInternal = SAPbobsCOM.BoYesNoEnum.tYES;
                oMsg.Recipients.SendEmail = SAPbobsCOM.BoYesNoEnum.tNO;
                oMsg.Priority = SAPbobsCOM.BoMsgPriorities.pr_High;


                if (oMsg.Add() != 0)
                {
                   // Console.WriteLine("failed");
                }
                else
                {
                    //Console.WriteLine("added");
                }
            }
            catch (Exception ex)
            {
            }
            finally
            {
                MarshallObject(oMsg);
            }
        }



        public int Check_If_Customer_Exists(string DBName,string CardCode, string SAPUserName, string SAPPassword) 
        {

            {
                Connect_To_SAP connect = new Connect_To_SAP();
                oCompany = connect.ConnectSAPDB(DBName, SAPUserName, SAPPassword);
                int results = 0;
                Recordset QueryObject = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);
                //  string querystring = "SELECT   FatherCard FROM OCRD WHERE FatherCard = '" + FatherCard + "' and  CardCode ='"+ CardCode +"'";

                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystring = "SELECT  COUNT(\"CardCode\") FROM \""+ DBName+ "\" + \".OCRD\"  WHERE  \"CardCode\" ='" + Sanitize(CardCode) + "'";
                }
                else
                {
                    querystring = "SELECT  COUNT(CardCode)FROM  "  + Sanitize(DBName) + ".[dbo]." + "OCRD (nolock) WHERE  CardCode ='" + Sanitize(CardCode) + "'";
                }
               // string querystring = 
                QueryObject.DoQuery(querystring);
                results = Convert.ToInt32(QueryObject.Fields.Item(0).Value);
                return results;
            }
        }

        public void Create_Customer(string CardCode, string CardName)
        {
            try {
                SAPbobsCOM.BusinessPartners sboBP = (SAPbobsCOM.BusinessPartners)oCompany.GetBusinessObject(SAPbobsCOM.BoObjectTypes.oBusinessPartners);
                sboBP.CardCode = CardCode;
                sboBP.CardName = CardName;
                sboBP.CardType = BoCardTypes.cCustomer;
                


                if (sboBP.Add() != 0)
                {

                    oCompany.GetLastError(out nErr, out erMsg);
                    //SAP_SEND_MESSAGE("Customer Added", "Customer Added from API", "Customer Name", CardCode, "2", CardCode, "", "", "", "");
                    //SendSAPMessage("Customer Added", "A Customer with  code " + CardCode + " and  name  " + CardName + " was created by API kindly update Customer Group and Payment Terms Accordingly ");

                }
                else
                {
                    SAP_SEND_MESSAGE("Customer Added", "Customer Added from API Kindly update Customer Group and other relevant fields ", "Customer Name", CardCode, "2", CardCode, "", "", "", "");
                    //SendSAPMessage("Customer Added", "A Customer with  code " + CardCode + " and  name  " + CardName + " was created by API kindly update Customer Group and Payment Terms Accordingly ");
                   // MarshallObject(sboBP);
                }
                MarshallObject(sboBP);

            }
            catch (Exception ex)
            {


            }


        }
        private bool CheckIfExists(string DBName,string CardCode, string CardType)

        {
            bool output = false;

            int results = 0;
            if (CardType == "Customer")
            {
                CardType = "C";
            }
            Recordset QueryObject = (Recordset)oCompany.GetBusinessObject(BoObjectTypes.BoRecordset);

           // string querystring = "SELECT  Count(CardCode)  as CardCode_Count  FROM OCRD (nolock)  WHERE CardCode = '" + Sanitize(CardCode) + "'   and  CardType ='" + Sanitize(CardType) + "'";
            if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                querystring = "SELECT  Count(\"CardCode\")  AS \"CardCode_Count\"   \""  + Sanitize(DBName) + "\" + \".OCRD\"  WHERE \"CardCode\" = '" + Sanitize(CardCode) + "'   and  \"CardType\" ='" + Sanitize(CardType) + "'";

            }
            else
            {
                querystring = "SELECT  Count(CardCode)FROM  "  + Sanitize(DBName) + ".[dbo]." + "OCRD (nolock) WHERE  CardCode ='" + Sanitize(CardCode) + "'";
            }

            QueryObject.DoQuery(querystring);
            results = Convert.ToInt32(QueryObject.Fields.Item(0).Value.ToString());

            if (results == 0)
            {
                output = false;
            }
            else
            {
                output = true;
            }

            return output;
        }

        [Authorize(Roles = "SuperAdmin, Admin, User")]
        //[HttpPost]
        [HttpGet]
        [Route("api/SAP/{DBName}/GetBusinessPartnerByCode/{CardCode}")]
        public HttpResponseMessage GetBusinessPartnerByCode(string DBName, string CardCode)
        {
            try
            {
                List<BusinessPartner_Master> customers = new List<BusinessPartner_Master>();
                //SqlParameter[] myCardCode = new SqlParameter[1];
                //myCardCode[0] = new SqlParameter("@CardCode", CardCode);

                if (DbServerType == "SAPHANA")
                {
                    //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                    querystring = "select top 1 T0.\"CardCode\", T0.\"CardName\", T0.\"Balance\", T0.\"Currency\"  FROM \""  + Sanitize(DBName) + "\" + \".OCRD\" t0  where  T0.\"CardCode\"='" + Sanitize(CardCode) + "'";

                }
                else
                {
                    querystring = "select top 1 T0.CardCode, T0.CardName, T0.Balance, T0.Currency from  "  + Sanitize(DBName) + ".[dbo]." + "ocrd t0 (nolock) where  T0.CardCode='" + Sanitize(CardCode) + "'";
                }

                DataTable dt = GetData( querystring);
                for (int i = 0; i < dt.Rows.Count; i++)
                {

                    BusinessPartner_Master customer = new BusinessPartner_Master
                    {

                        CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
                        ,
                        CardName = Convert.ToString(dt.Rows[i]["CardName"])
                        ,
                        Balance = Convert.ToDouble(dt.Rows[i]["Balance"])
                        ,
                        Currency = Convert.ToString(dt.Rows[i]["Currency"])
                        ,
                        Bill_To_Address = GetContact(DBName,Convert.ToString(dt.Rows[i]["CardCode"]))
                    };
                    customers.Add(customer);
                }

                var json = JsonConvert.SerializeObject(customers, Newtonsoft.Json.Formatting.Indented);
                var response = Request.CreateResponse(HttpStatusCode.OK);
                response.Content = new StringContent(json, Encoding.UTF8, "application/json");
                return response;
            }
            catch (Exception ex)
            {

                HttpResponseMessage exeption_response = null;
                exeption_response.Content = new StringContent(ex.Message, Encoding.UTF8, "application/json");
                return exeption_response;
            }
        }

        public List<BillToAddress> GetContact(string DBName, string customerId)
        {

            List<BillToAddress> billtoaddress = new List<BillToAddress>();

            if (DbServerType == "SAPHANA")
            {
                //querystring = "select case WHEN T1.\"OnHand\"  >= '" + Sanitize(Quantity) + "' THEN  'Y' ELSE 'N' END AS \"QuantityOk\" from \"OITM\" T0   INNER JOIN \"OITW\"  T1  ON T0.\"ItemCode\" = T1.\"ItemCode\" INNER JOIN \"OWHS\" T2 ON T2.\"WhsCode\" = T1.\"WhsCode\" WHERE  T0.\"ItemCode\" = '" + Sanitize(ItemCode) + "' and T2.\"WhsCode\" = '" + Sanitize(WarehouseCode) + "'";
                querystring = "select  T1.\"CardCode\", T1.\"Name\", T1.\"Title\", T1.\"Position\", T1.\"Address\", T1.\"Tel1\", T1.\"Cellolar\", T1.\"E_MailL\", T1.\"Active\" from  \""  + Sanitize(DBName) + "\" + \".OCPR\" T1 INNER JOIN" +
                           "  \""  + Sanitize(DBName) + "\" + \".OCRD\"  T0  ON  T0.\"CardCode\" =T1.\"CardCode\"  Where T0.\"CardCode\" ='" + (customerId) + "'";
            }
            else
            {
                querystring = "select  ISNULL(T0.CardCode,'')'CardCode', ISNULL(T1.Name,'')'Name', ISNULL(T1.Title,'') 'Title',"+
                   " ISNULL(T1.Position,'') 'Position', ISNULL(T1.Address,'') 'Address' ,ISNULL( T1.Tel1,'') 'Tel1',ISNULL( T1.Cellolar,'') 'Cellolar'," +
                    " ISNULL(T1.E_MailL,'') 'E_MailL',ISNULL( T1.Active,'') 'Active' from  "  + Sanitize(DBName) + ".[dbo]." + "OCPR T1(nolock) " +
                   " INNER JOIN "  + Sanitize(DBName) + ".[dbo]." + "OCRD  T0  ON  T0.CardCode =T1.CardCode  Where T0.CardCode ='" + (customerId) + "'";
            }
            if (customerId.Length > 1)
            {
                //querystring= querystring
                 DataTable dt = GetData(querystring);
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                billtoaddress.Add(new BillToAddress
                {
                    CardCode = Convert.ToString(dt.Rows[i]["CardCode"])
                    ,
                    Name = Convert.ToString(dt.Rows[i]["Name"])
                    ,
                    Title = Convert.ToString(dt.Rows[i]["Title"])
                    ,
                    Position = Convert.ToString(dt.Rows[i]["Position"])
                    ,
                    Address = Convert.ToString(dt.Rows[i]["Address"])
                    ,
                    Tel1 = Convert.ToString(dt.Rows[i]["Tel1"])
                      ,
                    Cellolar = Convert.ToString(dt.Rows[i]["Cellolar"])
                    ,
                    E_MailL = Convert.ToString(dt.Rows[i]["E_MailL"])
                    ,
                    Active = Convert.ToString(dt.Rows[i]["Active"])
                });
            }
            }
            return billtoaddress;
        }

        private DataTable GetData(string query)
        {

            string conString = ConfigurationManager.ConnectionStrings["constr"].ConnectionString  + @"Database=" + DBName.Trim() + ";" ;
            if(!string.IsNullOrEmpty(query))
                { 
                 query = query;
                }
            else 
            {
                query = ""; 
            }
           
             OdbcCommand cmd = new OdbcCommand(query);
            using (OdbcConnection con = new OdbcConnection(conString))
            {
                using (OdbcDataAdapter sda = new OdbcDataAdapter())
                {
                    cmd.Connection = con;
                    cmd.CommandTimeout = 4000000;
                    sda.SelectCommand = cmd;
                    using (DataTable dt = new DataTable())
                    {
                        sda.Fill(dt);
                        return dt;

                    }
                }
            }
        }

        public string Sanitize_Errors(string value)
        {
            List<string> words = new List<string> { "--", ";","," ,@"\\",@"//","[","]", "\"",":" };
           // .Replace("[", "").Replace("\\", "").Replace("//", "").Replace("]", "").Replace(",", "").Replace(";", "");


            foreach (string _mystring in words)
            {
                if (value.Contains(_mystring))
                {
                    value = value.Replace(_mystring, "");
                }
            }
            return value;
        }

        public string Sanitize(string value)
        {
            List<string> words = new List<string> { "--", ";", "drop", "truncate", "create", "call", "alter", "exec", "execute", "," };
            // .Replace("[", "").Replace("\\", "").Replace("//", "").Replace("]", "").Replace(",", "").Replace(";", "");


            foreach (string _mystring in words)
            {
                if (value.Contains(_mystring))
                {
                    value = value.ToLower().Replace(_mystring, "");
                }
            }
            return value;
        }




        public string DataTableToJSONWithStringBuilder(DataTable table)
        {
            var JSONString = new StringBuilder();
            string dbqt = @"""";
            if (table.Rows.Count > 0)
            {
                JSONString.Append("[");
                for (int i = 0; i < table.Rows.Count; i++)
                {
                    JSONString.Append("{");
                    for (int j = 0; j < table.Columns.Count; j++)

                    {
                        bool is_Numeric = IsNumeric(table.Rows[i][j].ToString());
                        if (j < table.Columns.Count - 1)
                        {

                            if (is_Numeric == true)
                            {

                                JSONString.Append("" + dbqt + table.Columns[j].ColumnName.ToString() + dbqt + ":" + "" + table.Rows[i][j].ToString().Replace(dbqt, "").Replace("'", "").Trim() + ",");
                            }
                            else
                            {
                                JSONString.Append("" + dbqt + table.Columns[j].ColumnName.ToString() + dbqt + ":" + "" + dbqt + table.Rows[i][j].ToString().Replace(dbqt, "").Replace("'", "").Trim() + dbqt + ",");

                            }

                            // JSONString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\",");
                        }
                        else if (j == table.Columns.Count - 1)
                        {
                            if (is_Numeric == true)
                            {

                                JSONString.Append("" + dbqt + table.Columns[j].ColumnName.ToString() + dbqt + ":" + "" + table.Rows[i][j].ToString().Replace(dbqt, "").Replace("'", "").Trim() + "");
                            }
                            else
                            {
                                JSONString.Append("" + dbqt + table.Columns[j].ColumnName.ToString() + dbqt + ":" + "" + dbqt + table.Rows[i][j].ToString().Replace(dbqt, "").Replace("'", "").Trim() + dbqt + "");

                            }
                            // JSONString.Append("\"" + dbqt+ table.Columns[j].ColumnName.ToString() + dbqt + "\":" + "\"" + table.Rows[i][j].ToString() + "\"");
                        }
                    }
                    if (i == table.Rows.Count - 1)
                    {
                        JSONString.Append("}");
                    }
                    else
                    {
                        JSONString.Append("},");
                    }
                }
                JSONString.Append("]");
            }
            return JSONString.ToString();
        }
        private bool IsNumeric(string str)
        {
            float f;
            return float.TryParse(str, out f);
        }

   

}
}
