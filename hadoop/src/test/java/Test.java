import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class Test {

    public static native void print();


    public static void main(String[] args) throws IOException {
        String s = "学霸情侣直博清华 包揽专业前两名";
        StringReader sr = new StringReader(s);
        IKSegmenter ikSegmenter = new IKSegmenter(sr,true);
        Lexeme word = null;
        while ((word = ikSegmenter.next()) != null ){
            String w = word.getLexemeText();
            System.out.println(w);
        }
    }
}
