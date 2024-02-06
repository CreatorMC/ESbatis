package com.creator.mybatis;

import com.alibaba.fastjson.JSON;
import com.creator.mybatis.io.Resources;
import com.creator.mybatis.session.SqlSession;
import com.creator.mybatis.session.SqlSessionFactoryBuilder;
import com.creator.mybatis.session.SqlSessionFactory;
import org.junit.Test;

import java.io.Reader;

public class ORMTest {

    @Test
    public void test_queryUserInfoById() {
        String resource = "mybatis-config-datasource.xml";
        Reader reader;
        try {
            reader = Resources.getResourceAsReader(resource);
            SqlSessionFactory sqlMapper = new SqlSessionFactoryBuilder().build(reader);
            SqlSession session = sqlMapper.openSession();
            try {
                Article article = session.selectOne("com.creator.mybatis.IUserDao.queryUserInfoById", 3L);
                System.out.println(article.toString());
            } finally {
                session.close();
                reader.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
