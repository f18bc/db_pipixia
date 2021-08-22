/* 
Copyright [2019-04-12] <pingfang> 
brief: sql语句生成器, 从TC_Mysql.h 迁移过来，调整成static方法，并增加了几个辅助方法
*/

#ifndef _SQL_BUILDER_H_
#define _SQL_BUILDER_H_

#include <util/tc_common.h>
#include "DecimalCast.h"
#include <string>

using namespace std;


class SqlBuilder
{

public:

	// 列类型
	enum ColumnType
	{
		DB_INT    = 0, // 数字类型
		DB_STR    = 1, // 字符串类型
        DB_VECTOR = 2, // 列表类型
	};

	// 列的值(包含列类型)
	struct ColumnValue
	{
		ColumnType type;
		string value;
    
		ColumnValue()
		{
			type = DB_INT;
		}
	};

public:

    /**
     * 构造批量insert语句
     *
     * @param sTableName 表明
     * @param v_mpColumns 列名/值对
     * @return string Insert-SQL语句
     */
    static string buildBatchInsertSQL(const string &sTableName, const vector<map<string, ColumnValue> > &v_mpColumns);

	/**
	* @brief 构造Insert-SQL语句.
	*
	* @param sTableName  表名
	* @param mpColumns 列名/值对
	* @return           string Insert-SQL语句
	*/
	static string buildInsertSQL(const string& sTableName, const map<string, ColumnValue>& mpColumns, bool bIgnore = false);


    /**
    * @brief 构造Replace-SQL语句.
    *
    * @param sTableName  表名
    * @param mpColumns  列名/值对
    * @return           string Replace-SQL语句
    */
    static string buildReplaceSQL(const string& sTableName, const map<string, ColumnValue>& mpColumns);
    

    /**
    * @brief 构造Update-SQL语句.
    *
    * @param sTableName  表名
    * @param mpColumns   列名/值对
    * @param sWhereFilter  where子语句(不包含where关键字)
    * @return            string Update-SQL语句
    */
    static string buildUpdateSQL(const string& sTableName, const map<string, ColumnValue>& mpColumns, const string& sWhereFilter);

    /**
    * @brief 构造insert-Update-SQL语句.
    *
    * @param sTableName  表名
    * @param mpColumns   列名/值对 要插入的列的值
    * @param updateColumns  主键重复后, 要更新的列的值
    * @return            string Update-SQL语句
    */
    static string buildInsertDupUpdateSQL(const string &sTableName, const map<string, SqlBuilder::ColumnValue> &mpInsertColumns,
                                            const map<string, SqlBuilder::ColumnValue> &mpUpdateColumns);
    /**
    * @brief 构造where子语句
    *
    * @param mpColumns   列名/值对
    * @return            string where子语句(不包含where关键字)
    */
    static string buildWhereFilter(const map<string, ColumnValue>& mpColumns);


    // 往 mpColumns 中添加字段
	// 注意：如果sql中需要用到now()函数, 譬如Fcreate_time = now(), 可以将type指定为DB_INT，这样拼sql时不会添加引号
    static void addField(map<string, ColumnValue>& mpColumns, const string& key, ColumnType type, const string& value);

    // 重写一个适合指定double类型高精度的函数 precision_of_str传入的指定保留精度
    // 目前tars::TC_Common::tostr(double)只支持最高6位小数精度
    template <typename T>
    static void addField(map<string, ColumnValue>& mpColumns, const string& key, const T& value, unsigned precision_of_str)
    {
        ColumnValue stColumnValue;
        // 数值或枚举，type设置为DB_INT
        if (std::is_arithmetic<T>::value || std::is_enum<T>::value)
        {
            stColumnValue.type  = DB_INT;
            DecimalCast(value, stColumnValue.value, precision_of_str);
        }
        else
        {
            stColumnValue.type = DB_STR;
            stColumnValue.value = value;
        }
        mpColumns.insert(make_pair(key, stColumnValue));
    }


    // 往 mpColumns 中添加字段, 类型自适应
    template <typename T>
    static void addField(map<string, ColumnValue>& mpColumns, const string& key, const T& value)
    {
        ColumnValue stColumnValue;
        // 数值或枚举，type设置为DB_INT
        if (std::is_arithmetic<T>::value || std::is_enum<T>::value)
        {
            stColumnValue.type  = DB_INT;
            stColumnValue.value = tars::TC_Common::tostr(value);
        }
        else
        {
            stColumnValue.type = DB_STR;
            stColumnValue.value = value;
        }
        mpColumns.insert(make_pair(key, stColumnValue));
        
    }

	// // 以下几个是针对字符串的重载函数
    // // 注意这里没有针对 unsigned char * 进行重载， 应避免使用 unsigned char * 
	// static void addField(map<string, ColumnValue>& mpColumns, const string& key, const string& value);
	// static void addField(map<string, ColumnValue>& mpColumns, const string& key, string& value);
	// static void addField(map<string, ColumnValue>& mpColumns, const string& key, const char * value);
	// static void addField(map<string, ColumnValue>& mpColumns, const string& key, char * value);

    /**
     *  @brief 字符转义.
     *
     * @param sFrom  源字符串
     * @return       输出字符串
     */
    static string escapeString(const string& sFrom);

    // 兼容int类型的转移
    static string escapeString(const int& sFrom)
    {
        return std::to_string(sFrom);
    }

    // 往 mpColumns 中添加sql的in条件字段
    // 例如：in (1,2,3,4) or in ("a","b","c")
    template <typename T>
    static void addCondition(map<string, ColumnValue>& mpColumns, const string& key, const vector<T>& value)
    {
        if (value.empty()) return;

        ColumnValue stColumnValue;
        stColumnValue.type = DB_VECTOR;

        ostringstream ss;
        size_t len = value.size();

        // 字段引号处理
        if (std::is_arithmetic<T>::value || std::is_enum<T>::value)
        {
            ss << "(";
            for (size_t i = 0; i < len; i++)
            {
                if (i < len - 1)
                {
                    ss << value[i] << ",";
                }
                else
                {
                    ss << value[i] << ")";
                }
            }
            stColumnValue.value = ss.str();
        }
        else
        {
            ss << "(";
            for (size_t i = 0; i < len; i++)
            {
                if (i < len - 1)
                {
                    ss << "'" << escapeString(value[i]) << "',";
                }
                else
                {
                    ss << "'" << escapeString(value[i]) << "')";
                }

            }
            stColumnValue.value = ss.str();
        }
        mpColumns.insert(make_pair(key, stColumnValue));
    }
    
    /**
	* 构造IN子语句
	* 把vector中的元素合并为一个字符串，该字符串可用于sql IN (...) 条件查询
	* @param[in]       vItem       需合并的元素的vector，元素须能支持<<操作符
	* @return          合并的字符串
	* @see 
	*/
    template <typename T>
	static std::string 		buildInField(const std::vector<T>& vItem)
    {
		std::stringstream ssStrJoined;
        std::string sSplitStr = ",";
        std::string sQuoteStr;

        // 字段引号处理
        if (std::is_arithmetic<T>::value || std::is_enum<T>::value)
        {
            sQuoteStr = "";
            for (size_t i = 0; i < vItem.size(); ++i)
            {
                if (i != 0)
                {
                    ssStrJoined << sSplitStr;
                }
                ssStrJoined << sQuoteStr << vItem[i] << sQuoteStr;
            }
        }
        else
        {
            sQuoteStr = "'";
            for (size_t i = 0; i < vItem.size(); ++i)
            {
                if (i != 0)
                {
                    ssStrJoined << sSplitStr;
                }
                ssStrJoined << sQuoteStr << escapeString(vItem[i]) << sQuoteStr;
            }
        }
		return ssStrJoined.str();
	}

};

#endif  // _SQL_BUILDER_H_
