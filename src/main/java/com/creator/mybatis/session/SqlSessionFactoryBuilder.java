package com.creator.mybatis.session;

import com.creator.mybatis.builder.xml.XMLMapperEntityResolver;
import com.creator.mybatis.io.Resources;
import com.creator.mybatis.parsing.XNode;
import com.creator.mybatis.session.defaults.DefaultSqlSessionFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlSessionFactoryBuilder {

    public DefaultSqlSessionFactory build(Reader reader) {
        SAXReader saxReader = new SAXReader();
        try {
            saxReader.setEntityResolver(new XMLMapperEntityResolver());
            Document document = saxReader.read(new InputSource(reader));
            Configuration configuration = parseConfiguration(document.getRootElement());
            return new DefaultSqlSessionFactory(configuration);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 配合 esbatis-spring-boot-starter
     * @param connection
     * @param packageSearchPath
     * @return
     * @throws IOException
     * @throws DocumentException
     */
    public DefaultSqlSessionFactory build(Connection connection, String packageSearchPath) throws IOException, DocumentException {
        Configuration configuration = new Configuration();
        configuration.setConnection(connection);
        // 读取配置
        ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resourcePatternResolver.getResources(packageSearchPath);
        List<Element> list = new ArrayList<>(resources.length);
        for(Resource resource : resources) {
            Document document = new SAXReader().read(new InputSource(new InputStreamReader(resource.getInputStream())));
            list.add(document.getRootElement());
        }
        configuration.setMapperElement(mapperElementNoResource(list));
        return new DefaultSqlSessionFactory(configuration);
    }

    /**
     * 解析配置
     * @param root
     * @return
     */
    private Configuration parseConfiguration(Element root) {
        Configuration configuration = new Configuration();
        configuration.setDataSource(dataSource(root.selectNodes("//dataSource")));
        configuration.setConnection(connection(configuration.getDataSource()));
        configuration.setMapperElement(mapperElement(root.selectNodes("mappers")));
        return configuration;
    }

    /**
     * 获取数据源配置信息
     * @param list
     * @return
     */
    private Map<String, String> dataSource(List<Element> list) {
        Map<String, String> dataSource = new HashMap<>();
        Element element = list.get(0);
        List content = element.content();
        for (Object o : content) {
            Element e = (Element) o;
            String name = e.attributeValue("name");
            String value = e.attributeValue("value");
            dataSource.put(name, value);
        }
        return dataSource;
    }

    /**
     * 获取连接
     * @param dataSource
     * @return
     */
    private Connection connection(Map<String, String> dataSource) {
        try {
//            Class.forName(dataSource.get("driver"));
            return DriverManager.getConnection(dataSource.get("url"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取 SQL 语句信息
     * @param list
     * @return
     */
    private Map<String, XNode> mapperElement(List<Element> list) {
        Map<String, XNode> map = new HashMap<>();
        Element element = list.get(0);
        List content = element.content();
        for (Object o : content) {
            Element e = (Element) o;
            String resource = e.attributeValue("resource");

            try {
                Reader reader = Resources.getResourceAsReader(resource);
                SAXReader saxReader = new SAXReader();
                Document document = saxReader.read(new InputSource(reader));
                Element root = document.getRootElement();
                parseElement(map, root);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
        return map;
    }

    /**
     * 从对应的 mapper.xml 中直接获取 SQL 语句信息
     * @param list
     * @return
     */
    private Map<String, XNode> mapperElementNoResource(List<Element> list) {
        Map<String, XNode> map = new HashMap<>();
        for (Element o : list) {
            try {
                parseElement(map, o);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return map;
    }

    private void parseElement(Map<String, XNode> map, Element root) {
        //命名空间
        String namespace = root.attributeValue("namespace");

        // SELECT
        List<Element> selectNodes = root.selectNodes("select");
        for (Element node : selectNodes) {
            String id = node.attributeValue("id");
            String parameterType = node.attributeValue("parameterType");
            String resultType = node.attributeValue("resultType");
            String sql = node.getText();

            // ? 匹配
            Map<Integer, String> parameter = new HashMap<>();
            Pattern pattern = Pattern.compile("(#\\{(.*?)})");
            Matcher matcher = pattern.matcher(sql);
            for (int i = 1; matcher.find(); i++) {
                String g1 = matcher.group(1);
                String g2 = matcher.group(2);
                parameter.put(i, g2);
                sql = sql.replace(g1, "?");
            }

            XNode xNode = new XNode();
            xNode.setNamespace(namespace);
            xNode.setId(id);
            xNode.setParameterType(parameterType);
            xNode.setResultType(resultType);
            xNode.setSql(sql);
            xNode.setParameter(parameter);

            map.put(namespace + "." + id, xNode);
        }
    }
}
