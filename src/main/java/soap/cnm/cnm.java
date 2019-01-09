package soap.cnm;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ZhangPY on 2018/11/16
 * Belong Organization OVERUN-9299
 * overun9299@163.com
 */
public class cnm {

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    // 索引保存目录
    private String indexDir = "F:\\wdnmd\\cnm2";
    private static IndexSearcher searcher = null;
    //创建分词器
    private static Analyzer analyzer = new IKAnalyzer(true);

    /**
     * 获取数据库数据
     * @param queryStr 需要检索的关键字
     * @return
     * @throws Exception
     */
    public List<DocumentShow> getResult(String queryStr) throws Exception {
        System.out.println("查询内容---"+queryStr);
        System.out.println();
        List<DocumentShow> result = null;
        List<DocumentShow> rs = new ArrayList<>();
        conn = JdbcUtil.getConnection();
        if (conn==null) {
            System.out.println("cnm连接失败");
        }
        String sql = "select * from document";
        stmt = conn.createStatement();
        try {
            ResultSet r = stmt.executeQuery(sql);
            while (r.next()) {
                DocumentShow d = new DocumentShow();
                d.setId(r.getLong("id"));
                d.setTitle(r.getString("title"));
                d.setTag(r.getString("tag"));
                rs.add(d);
            }
        } catch (Exception e) {

        }


        this.createIndex(rs);
        TopDocs topDocs = this.search(queryStr,rs.size());
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        result = this.addHits2List(scoreDocs);
        return result;
    }




    /**
     * 为数据库检索数据创建索引
     * @param
     * @throws Exception
     */
    private void createIndex(List<DocumentShow> rs) throws Exception {
        // 创建或打开索引
        File f = new File(indexDir);
        try {
            cnm.deleteFile(f);
        } catch (Exception e) {
        }
        Directory directory = FSDirectory.open(new File(indexDir));
        // 创建IndexWriter
        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_46,
                analyzer);
        IndexWriter indexWriter = new IndexWriter(directory, conf);
        // 遍历ResultSet创建索引
        for (int i = 0; i<rs.size();i++) {

            // 创建document并添加field
            Document doc = new Document();
            doc.add(new TextField("id", String.valueOf(rs.get(i).getId()), Field.Store.YES));
            doc.add(new TextField("tag", rs.get(i).getTag(),
                    Field.Store.YES));
            doc.add(new TextField("title", rs.get(i).getTitle(),
                    Field.Store.YES));
//            doc.add(new TextField("c2", rs.get(i).getCreateDate().toString(),
//                    Field.Store.YES));
//            doc.add(new TextField("type", rs.get(i).getType(),
//                    Field.Store.YES));
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
    private TopDocs search(String queryStr,Integer num) throws Exception {
        //创建或打开索引目录
        Directory directory = FSDirectory.open(new File(indexDir));
        IndexReader reader = IndexReader.open(directory);

        searcher = new IndexSearcher(reader);

        //使用查询解析器创建Query
        BooleanClause.Occur[] flags = {
                BooleanClause.Occur.SHOULD,
                BooleanClause.Occur.SHOULD};
        Query parse = MultiFieldQueryParser.parse(Version.LUCENE_46, new String[]{queryStr, queryStr}, new String[]{"title", "title"},flags, analyzer);
        //从索引中搜索得到排名前10的文档
        TopDocs topDocs = searcher.search(parse,num);
        return topDocs;
    }

    /**
     * 将检索结果添加到List中
     * @param scoreDocs
     * @return
     * @throws Exception
     */
    private List<DocumentShow> addHits2List(ScoreDoc[] scoreDocs) throws Exception {
        List<DocumentShow> listBean = new ArrayList<DocumentShow>();
        DocumentShow bean = null;
        for (int i = 0; i < scoreDocs.length; i++) {
            int docId = scoreDocs[i].doc;
            Document doc = searcher.doc(docId);
            bean = new DocumentShow();
            bean.setId(Long.parseLong(doc.get("id")));
            bean.setTag(doc.get("tag"));
            bean.setTitle(doc.get("title"));
//            bean.setC(Date.valueOf(doc.get("c2")));
//            bean.setType(doc.get("type"));
            listBean.add(bean);
        }
        return listBean;
    }

    public static void main(String[] args) {
        cnm logic = new cnm();
        try {
            Long startTime = System.currentTimeMillis();
            List<DocumentShow> result = logic.getResult("文");
            int i = 0;
            for (DocumentShow bean : result) {
                if (i == 10)
                    break;
                System.out.println(bean);
            }

            System.out.println("searchBean.result.size : " + result.size());
            Long endTime = System.currentTimeMillis();
            System.out.println("查询所花费的时间为：" + (endTime - startTime) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
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
}
