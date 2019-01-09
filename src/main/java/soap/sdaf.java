package soap;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class sdaf {
    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    // 索引保存目录
    private String indexDir = "F:\\wdnmd\\cnm6";
    private static IndexSearcher searcher = null;
    //创建分词器
    private static Analyzer analyzer = new IKAnalyzer(true);

    /**
     * 获取数据库数据
     * @param queryStr 需要检索的关键字
     * @return
     * @throws Exception
     */
    public List<UserInfo> getResult(String queryStr) throws Exception {
        List<UserInfo> result = null;
        conn = JdbcUtil.getConnection();
        if (conn == null) {
            throw new Exception("数据库连接失败！");
        }
        String sql = "select * from document";
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            // 给数据库创建索引,此处执行一次，不要每次运行都创建索引
            // 以后数据有更新可以后台调用更新索引
            List<UserInfo> l = new ArrayList<>();

            while (rs.next()) {
                UserInfo u = new UserInfo();
                u.setId(Long.parseLong(rs.getString("id")));
                u.setTitle(rs.getString("title"));
                u.setTag(rs.getString("tag"));
                l.add(u);

            }
            this.createIndex(l);
            TopDocs topDocs = this.search(queryStr);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            result = this.addHits2List(scoreDocs);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("数据库查询sql出错！ sql : " + sql);
        } finally {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (conn != null)
                conn.close();
        }
        return result;
    }

    public static boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (File file : dirFile.listFiles()) {
                deleteFile(file);
            }
        }

        return dirFile.delete();
    }


    /**
     * 为数据库检索数据创建索引
     * @param
     * @throws Exception
     */
    private void createIndex(List<UserInfo> l) throws Exception {
        // 创建或打开索引
        File f = new File(indexDir);
        sdaf.deleteFile(f);

        Directory directory = FSDirectory.open(new File(indexDir));
        // 创建IndexWriter
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_46,
                analyzer);
        IndexWriter indexWriter = new IndexWriter(directory, conf);
        // 遍历ResultSet创建索引
            // 创建document并添加field
        for (int i = 0 ;i<l.size();i++){

            Document doc = new Document();
            doc.add(new TextField("id", String.valueOf(l.get(i).getId()), Field.Store.YES));
            doc.add(new TextField("tag", l.get(i).getTag(),
                    Field.Store.YES));
            doc.add(new TextField("title", l.get(i).getTitle(),
                    Field.Store.YES));
            // 将doc添加到索引中
            indexWriter.addDocument(doc);
        }
        indexWriter.commit();
        indexWriter.close();
        directory.close();
    }

    /**
     * 检索索引
     * @param queryStr 需要检索的关键字
     * @return
     * @throws Exception
     */
    private TopDocs search(String queryStr) throws Exception {
        //创建或打开索引目录
        Directory directory = FSDirectory.open(new File(indexDir));
        IndexReader reader = IndexReader.open(directory);
        if (searcher == null) {
            searcher = new IndexSearcher(reader);
        }
        //使用查询解析器创建Query
        QueryParser parser = new QueryParser(Version.LUCENE_46, "title", analyzer);
        BooleanClause.Occur[] flags = {
                BooleanClause.Occur.SHOULD,
                BooleanClause.Occur.SHOULD};
        Query parse = MultiFieldQueryParser.parse(Version.LUCENE_46, new String[]{queryStr, queryStr}, new String[]{"tag", "title"},flags, analyzer);

        Query query = parser.parse(queryStr);
        //从索引中搜索得到排名前10的文档
        TopDocs topDocs = searcher.search(parse, 10);
        return topDocs;
    }

    /**
     * 将检索结果添加到List中
     * @param scoreDocs
     * @return
     * @throws Exception
     */
    private List<UserInfo> addHits2List(ScoreDoc[] scoreDocs) throws Exception {
        List<UserInfo> listBean = new ArrayList<UserInfo>();
        UserInfo bean = null;
        for (int i = 0; i < scoreDocs.length; i++) {
            int docId = scoreDocs[i].doc;
            Document doc = searcher.doc(docId);
            bean = new UserInfo();
            bean.setId(Long.parseLong(doc.get("id")));
            bean.setTag(doc.get("tag"));
            bean.setTitle(doc.get("title"));
            listBean.add(bean);
        }
        return listBean;
    }

    public static void main(String[] args) {
        sdaf logic = new sdaf();
        try {
            Long startTime = System.currentTimeMillis();
            List<UserInfo> result = logic.getResult("文档");
            int i = 0;
            for (UserInfo bean : result) {
                if (i == 10)
                    break;
                System.out.println("bean.name " + bean.getClass().getName()
                        + " : bean.id " + bean.getId() + " : bean.username "
                        + bean.getTag()+"title"+bean.getTitle());
                i++;
            }

            System.out.println("searchBean.result.size : " + result.size());
            Long endTime = System.currentTimeMillis();
            System.out.println("查询所花费的时间为：" + (endTime - startTime) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}