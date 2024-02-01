package com.creator.mybatis.session.defaults;

import com.creator.mybatis.parsing.XNode;
import com.creator.mybatis.session.SqlSession;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultSqlSession implements SqlSession {

    private Connection connection;
    private Map<String, XNode> mapperElement;

    public DefaultSqlSession(Connection connection, Map<String, XNode> mapperElement) {
        this.connection = connection;
        this.mapperElement = mapperElement;
    }

    @Override
    public <T> T selectOne(String statement) {
        try {
            XNode xNode = mapperElement.get(statement);
            PreparedStatement preparedStatement = connection.prepareStatement(xNode.getSql());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<T> objects = resultSet2Obj(resultSet, Class.forName(xNode.getResultType()));
            return objects.get(0);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T> T selectOne(String statement, Object parameter) {
        XNode xNode = mapperElement.get(statement);
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(xNode.getSql());
            buildParameter(preparedStatement, parameter, xNode.getParameter());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<T> objects = resultSet2Obj(resultSet, Class.forName(xNode.getResultType()));
            return objects.get(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T> List<T> selectList(String statement) {
        try {
            XNode xNode = mapperElement.get(statement);
            PreparedStatement preparedStatement = connection.prepareStatement(xNode.getSql());
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet2Obj(resultSet, Class.forName(xNode.getResultType()));
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T> List<T> selectList(String statement, Object parameter) {
        XNode xNode = mapperElement.get(statement);
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(xNode.getSql());
            buildParameter(preparedStatement, parameter, xNode.getParameter());
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet2Obj(resultSet, Class.forName(xNode.getResultType()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将结果封装为列表集合
     * @param resultSet
     * @param clazz
     * @return
     * @param <T>
     */
    private <T> List<T> resultSet2Obj(ResultSet resultSet, Class<?> clazz) {
        List<T> list = new ArrayList<>();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            //遍历行
            while(resultSet.next()) {
                T obj = (T) clazz.newInstance();
                //遍历当前行中的每列
                for (int i = 1; i <= columnCount; i++) {
                    //获取当前列的值
                    Object value = resultSet.getObject(i);
                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    //拼接得到对应的 setter 方法名
                    //columnName.substring(0,1).toUpperCase() 解释：
                    //数据库中属性字段假设为 "user_name"，而代码中对应包装对象的 setter 方法叫 setUserName。所以需要将单词的首字母转为大写
                    //考虑到非严谨的数据库表设计，如果本身字段名叫 "Name"，则不用将第一个字符转大写。故采用 jdk 中的函数，而不是让字符减 32
                    StringBuilder setMethod = new StringBuilder("set");
                    String[] words = columnName.split("_");
                    for (String word : words) {
                        setMethod.append(word.substring(0, 1).toUpperCase()).append(word.substring(1));
                    }
                    //反射拿到 setter 方法
                    Method method = null;
                    if(value instanceof Timestamp || value instanceof LocalDateTime) {
                        //时间戳类型转换为日期类型
                        method = clazz.getMethod(setMethod.toString(), java.util.Date.class);
                    } else if(null != value){
                        method = clazz.getMethod(setMethod.toString(), value.getClass());
                    }
                    //执行 setter 方法，给 obj 对象中的属性赋值
                    if (method != null) {
                        //将 LocalDateTime 转为 Date
                        if(value instanceof LocalDateTime) {
                            LocalDateTime localDateTime = (LocalDateTime) value;
                            value = java.util.Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
                        }
                        method.invoke(obj, value);
                    }
                }
                list.add(obj);
            }
        } catch (SQLException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 填充 preparedStatement 中的参数
     * @param preparedStatement
     * @param parameter
     * @param parameterMap
     * @throws SQLException
     * @throws IllegalAccessException
     */
    private void buildParameter(PreparedStatement preparedStatement, Object parameter, Map<Integer, String> parameterMap) throws SQLException, IllegalAccessException {
        int size = parameterMap.size();
        //单个参数
        if(parameter instanceof Long) {
            for (int i = 1; i <= size; i++) {
                preparedStatement.setLong(i, Long.parseLong(parameter.toString()));
            }
            return;
        }

        if (parameter instanceof Integer) {
            for (int i = 1; i <= size; i++) {
                preparedStatement.setInt(i, Integer.parseInt(parameter.toString()));
            }
            return;
        }

        if (parameter instanceof String) {
            for (int i = 1; i <= size; i++) {
                preparedStatement.setString(i, parameter.toString());
            }
            return;
        }
        //对象参数
        Map<String, Object> fieldMap = new HashMap<>();
        //得到当前字节码对象的不包括父类的字段数组，包括各种访问权限的。
        Field[] declaredFields = parameter.getClass().getDeclaredFields();
        for (Field field : declaredFields) {
            String name = field.getName();
            //设置为允许访问（避免字段是私有的造成不可访问）
            field.setAccessible(true);
            //获取参数对象此 field 字段的值
            Object obj = field.get(parameter);
            field.setAccessible(false);
            fieldMap.put(name, obj);
        }

        for (int i = 1; i <= size; i++) {
            //拿参数名
            String parameterDefine = parameterMap.get(i);
            //通过参数名得到上一步中对应参数的值
            Object obj = fieldMap.get(parameterDefine);

            if (obj instanceof Short) {
                preparedStatement.setShort(i, Short.parseShort(obj.toString()));
                continue;
            }

            if (obj instanceof Integer) {
                preparedStatement.setInt(i, Integer.parseInt(obj.toString()));
                continue;
            }

            if (obj instanceof Long) {
                preparedStatement.setLong(i, Long.parseLong(obj.toString()));
                continue;
            }

            if (obj instanceof String) {
                preparedStatement.setString(i, obj.toString());
                continue;
            }

            if (obj instanceof Date) {
                preparedStatement.setDate(i, (java.sql.Date) obj);
            }
        }

    }

    @Override
    public void close() {
        if(null == connection) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
