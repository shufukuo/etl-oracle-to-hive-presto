using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;


public class OracleQuery
{
    [Publish(NoLog = true)]
    public string Query(string sqlcmd, string is_transform_addr_isfloor = "false")
    {
        string str_out = "";

        DataTable dt = MyQuery(sqlcmd);


        if (is_transform_addr_isfloor.ToLower() == "true")
        {
            try
            {
                dt = transform_addr_isfloor(dt, "colname1");
            }
            catch (Exception exc)
            {

            }
            try
            {
                dt = transform_addr_isfloor(dt, "colname2");
            }
            catch (Exception exc)
            {

            }
        }


        if (dt.Columns.Contains("RN"))
        {
            dt.Columns.Remove("RN");
        }


        StringBuilder sb = new StringBuilder("");

        foreach (DataRow row in dt.Rows)
        {
            var obj_row = row.ItemArray;

            for (int i = 0; i < obj_row.Length; i++)
            {
                if (obj_row[i] is string)
                {
                    var tmp = obj_row[i].ToString().Trim().Replace("\t", "").Replace("\n", "").Replace("\r", "").Replace("\a", "").Replace("\b", "").Replace("\f", "").Replace("\v", "");
                    obj_row[i] = (tmp == " " || tmp == "") ? "NULL" : tmp;
                }
                else if (obj_row[i].GetType().Name == "DateTime")
                {
                    obj_row[i] = Convert.ToDateTime(obj_row[i]).ToString("yyyy-MM-dd HH:mm:ss");
                }
                else if (obj_row[i].ToString() == "")
                {
                    obj_row[i] = "NULL";
                }
            }

            sb.Append(string.Join("\t", obj_row) + "\n");
        }

        str_out = sb.ToString();


        return str_out;
    }


    DataTable transform_addr_isfloor(DataTable dt, string proc_colname)
    {
        int col_pos_addr = dt.Columns[proc_colname].Ordinal;
        dt.Columns[proc_colname].ColumnName = "addr_o";
        dt.Columns.Add(new DataColumn { ColumnName = proc_colname, DataType = typeof(string), DefaultValue = "-" });
        dt.Columns[proc_colname].SetOrdinal(col_pos_addr);

        List<string> lst_str_num_ch_low = new List<string> { "十樓", "一樓", "二樓", "三樓", "四樓", "五樓", "六樓", "七樓", "八樓", "九樓" };
        List<string> lst_str_num_ch_upp = new List<string> { "拾樓", "壹樓", "貳樓", "參樓", "叁樓", "肆樓", "伍樓", "陸樓", "柒樓", "捌樓", "玖樓" };
        lst_str_num_ch_low.AddRange(lst_str_num_ch_upp);

        foreach (DataRow row in dt.Rows)
        {
            string addr_o = row["addr_o"].ToString().Normalize(NormalizationForm.FormKC).Replace(" ", "");

            if ( 1== 0
                || Regex.Match(addr_o, @"\b\w*\d{1}(樓)\w*\b", RegexOptions.IgnoreCase).Success
                || Regex.Match(addr_o, @"\b\w*\d{1}(f)\w*\b", RegexOptions.IgnoreCase).Success
                )
            {
                row[proc_colname] = "1";
            }
            else
            {
                foreach (string s in lst_str_num_ch_low)
                {
                    if (addr_o.Contains(s))
                    {
                        row[proc_colname] = "1";
                        break;
                    }
                }
            }
        }

        dt.Columns.Remove("addr_o");

        return dt;
    }
}
