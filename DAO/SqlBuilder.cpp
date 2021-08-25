#include "SqlBuilder.h"
#include <string>


// using namespace tars;
string SqlBuilder::buildBatchInsertSQL(const string &sTableName, const vector<map<string, ColumnValue>> &v_mpColumns)
{
    ostringstream sColumnNames;
    string sColumnValueList;

    auto mpColumns = v_mpColumns[0];
    //拼接字段名
    for (auto item = mpColumns.begin(); item != mpColumns.end(); ++item)
    {
        if (item == mpColumns.begin())
        {
            sColumnNames << "`" << item->first << "`";
        }
        else
        {
            sColumnNames << ",`" << item->first << "`";
        }
    }

    //拼接数据
    for (unsigned int i = 0; i < v_mpColumns.size(); ++i)
    {
        ostringstream sColumnValues;
        map<string, ColumnValue> m_column = v_mpColumns[i];
        //遍历行数据
        for (auto m_field = m_column.begin(); m_field != m_column.end(); ++m_field)
        {
            //遍历字段
            if (m_field == m_column.begin())
            {
                if (m_field->second.type == DB_INT)
                {
                    sColumnValues << m_field->second.value;
                }
                else
                {
                    sColumnValues << "'" << escapeString(m_field->second.value) << "'";
                }
            }
            else
            {
                if (m_field->second.type == DB_INT)
                {
                    sColumnValues << "," + m_field->second.value;
                }
                else
                {
                    sColumnValues << ",'" + escapeString(m_field->second.value) << "'";
                }
            }
        }
        //拼接
        if (i == v_mpColumns.size() - 1)
        {
            sColumnValueList += "(" + sColumnValues.str() + ")";
        }
        else
        {
            sColumnValueList += "(" + sColumnValues.str() + "),";
        }
    }

    ostringstream os;
    os << "insert into " << sTableName << " (" << sColumnNames.str() << ") values " << sColumnValueList;
    return os.str();
}


string SqlBuilder::buildInsertSQL(const string &sTableName, const map<string, ColumnValue> &mpColumns, bool bIgnore /*= false*/)
{
    ostringstream sColumnNames;
    ostringstream sColumnValues;

    map<string, ColumnValue>::const_iterator itEnd = mpColumns.end();

    for (map<string, ColumnValue>::const_iterator it = mpColumns.begin(); it != itEnd; ++it)
    {
        if (it == mpColumns.begin())
        {
            sColumnNames << "`" << it->first << "`";
            if (it->second.type == DB_INT)
            {
                sColumnValues << it->second.value;
            }
            else
            {
                sColumnValues << "'" << escapeString(it->second.value) << "'";
            }
        }
        else
        {
            sColumnNames << ",`" << it->first << "`";
            if (it->second.type == DB_INT)
            {
                sColumnValues << "," + it->second.value;
            }
            else
            {
                sColumnValues << ",'" + escapeString(it->second.value) << "'";
            }
        }
    }

    ostringstream os;
	if (bIgnore)
	{
		os << "insert ignore into " << sTableName << " (" << sColumnNames.str() << ") values (" << sColumnValues.str() << ")";
	}
	else
	{
		os << "insert into " << sTableName << " (" << sColumnNames.str() << ") values (" << sColumnValues.str() << ")";
	}
   
    return os.str();
}

string SqlBuilder::buildReplaceSQL(const string &sTableName, const map<string, ColumnValue> &mpColumns)
{
    ostringstream sColumnNames;
    ostringstream sColumnValues;

    map<string, ColumnValue>::const_iterator itEnd = mpColumns.end();
    for (map<string, ColumnValue>::const_iterator it = mpColumns.begin(); it != itEnd; ++it)
    {
        if (it == mpColumns.begin())
        {
            sColumnNames << "`" << it->first << "`";
            if (it->second.type == DB_INT)
            {
                sColumnValues << it->second.value;
            }
            else
            {
                sColumnValues << "'" << escapeString(it->second.value) << "'";
            }
        }
        else
        {
            sColumnNames << ",`" << it->first << "`";
            if (it->second.type == DB_INT)
            {
                sColumnValues << "," + it->second.value;
            }
            else
            {
                sColumnValues << ",'" << escapeString(it->second.value) << "'";
            }
        }
    }

    ostringstream os;
    os << "replace into " << sTableName << " (" << sColumnNames.str() << ") values (" << sColumnValues.str() << ")";
    return os.str();
}


string SqlBuilder::buildUpdateSQL(const string &sTableName, const map<string, ColumnValue> &mpColumns, const string &sWhereFilter)
{
    ostringstream sColumnNameValueSet;

    map<string, ColumnValue>::const_iterator itEnd = mpColumns.end();

    for (map<string, ColumnValue>::const_iterator it = mpColumns.begin(); it != itEnd; ++it)
    {
        if (it == mpColumns.begin())
        {
            sColumnNameValueSet << "`" << it->first << "`";
        }
        else
        {
            sColumnNameValueSet << ",`" << it->first << "`";
        }

        if (it->second.type == DB_INT)
        {
            sColumnNameValueSet << "= " << it->second.value;
        }
        else
        {
            sColumnNameValueSet << "= '" << escapeString(it->second.value) << "'";
        }
    }

    ostringstream os;
    os << "update " << sTableName << " set " << sColumnNameValueSet.str() << " where " << sWhereFilter;

    return os.str();
}

string SqlBuilder::buildInsertDupUpdateSQL(const string &sTableName, const map<string, SqlBuilder::ColumnValue> &mpInsertColumns,
                                        const map<string, SqlBuilder::ColumnValue> &mpUpdateColumns)
{
    ostringstream sColumnNames;
    ostringstream sColumnValues;

    for (map<string, SqlBuilder::ColumnValue>::const_iterator it = mpInsertColumns.begin(); it != mpInsertColumns.end(); ++it)
    {
        if (it != mpInsertColumns.begin())
        {
            sColumnNames << ",";
            sColumnValues << ",";
        }

        sColumnNames << "`" << it->first << "`";
        if (it->second.type == SqlBuilder::DB_INT)
        {
            sColumnValues << it->second.value;
        }
        else
        {
            sColumnValues << "'" << SqlBuilder::escapeString(it->second.value) << "'";
        }
    }

    ostringstream sUpdateColumnValues; // ON DUPLICATE KEY UPDATE
    for (map<string, SqlBuilder::ColumnValue>::const_iterator it = mpUpdateColumns.begin(); it != mpUpdateColumns.end(); ++it)
    {
        if (it != mpUpdateColumns.begin())
        {
            sUpdateColumnValues << ", ";
        }

        if (it->second.type == SqlBuilder::DB_INT)
        {
            sUpdateColumnValues << "`" << it->first << "`=" << it->second.value;
        }
        else
        {
            sUpdateColumnValues << "`" << it->first << "`=" << "'" << SqlBuilder::escapeString(it->second.value) << "'";
        }
    }

    ostringstream os;
    os << "insert into " << sTableName << " (" << sColumnNames.str() << ") values (" << sColumnValues.str()
       << ") ON DUPLICATE KEY UPDATE " << sUpdateColumnValues.str();
    
    return os.str();
}

string SqlBuilder::buildWhereFilter(const map<string, ColumnValue>& mpColumns)
{
    ostringstream ssWhereFilter;

    for (map<string, ColumnValue>::const_iterator it = mpColumns.begin(); it != mpColumns.end(); ++it)
    {
        if (it == mpColumns.begin())
        {
            ssWhereFilter << "`" << it->first << "`";
        }
        else
        {
            ssWhereFilter << " and `" << it->first << "`";
        }

        if (it->second.type == DB_VECTOR)
        {
            ssWhereFilter << " in " << it->second.value;
        }
        else if (it->second.type == DB_INT)
        {
            ssWhereFilter << " = " << it->second.value;
        }
        else
        {
            ssWhereFilter << " = '" << escapeString(it->second.value) << "'";
        }
    }

    return ssWhereFilter.str();
}


// 往 mpColumns 中添加字段
void SqlBuilder::addField(map<string, ColumnValue>& mpColumns, const string& key, ColumnType type, const string& value)
{
    ColumnValue stColumnValue;
    stColumnValue.type  = type;
    stColumnValue.value = value;
    mpColumns.insert(make_pair(key, stColumnValue));
}

// void SqlBuilder::addField(map<string, ColumnValue>& mpColumns, const string& key, const string& value)
// {
// 	ColumnValue stColumnValue;
// 	stColumnValue.type = DB_STR;
// 	stColumnValue.value = value;
// 	mpColumns.insert(make_pair(key, stColumnValue));
// }

// void SqlBuilder::addField(map<string, ColumnValue>& mpColumns, const string& key, string& value)
// {
// 	ColumnValue stColumnValue;
// 	stColumnValue.type = DB_STR;
// 	stColumnValue.value = value;
// 	mpColumns.insert(make_pair(key, stColumnValue));
// }

// void SqlBuilder::addField(map<string, ColumnValue>& mpColumns, const string& key, const char * value)
// {
// 	ColumnValue stColumnValue;
// 	stColumnValue.type = DB_STR;
// 	stColumnValue.value = value;
// 	mpColumns.insert(make_pair(key, stColumnValue));
// }

// void SqlBuilder::addField(map<string, ColumnValue>& mpColumns, const string& key, char * value)
// {
// 	ColumnValue stColumnValue;
// 	stColumnValue.type = DB_STR;
// 	stColumnValue.value = value;
// 	mpColumns.insert(make_pair(key, stColumnValue));
// }


string SqlBuilder::escapeString(const string &sFrom)
{
    string sTo;
    string::size_type iLen = sFrom.length() * 2 + 1;
    char *pTo              = (char *)malloc(iLen);

    memset(pTo, 0x00, iLen);

    mysql_escape_string(pTo, sFrom.c_str(), sFrom.length());

    sTo = pTo;

    free(pTo);

    return sTo;
}
