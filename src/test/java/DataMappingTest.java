import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.zcu.kiv.nlp.ir.Comment;
import cz.zcu.kiv.nlp.ir.JavaAPIMain;
import org.junit.Test;
import sun.plugin.com.event.COMEventHandler;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DataMappingTest {

    /**
     * Correct date parser for format used in data.
     * @throws ParseException
     */
    @Test
    public void testDateFormatParser() throws ParseException {
        String dateStr = "Tue Feb 25 00:49:25 2020 UTC";
        DateFormat dateFormat = new SimpleDateFormat(JavaAPIMain.DATA_ORIGINAL_DATE_FORMAT, Locale.ENGLISH);
        Date d = dateFormat.parse(dateStr);
        System.out.println(d);
    }

    @Test
    public void testTransformDate() throws JsonProcessingException {
        String jsonData = "{\"username\":\"TwilitSky\", \"text\":\"They should be able to get it. They did last time except for Rand \\\"Baby Cracked Ribs\\\" Paul and Mike Lee of Utah.\", \"score\":25, \"timestamp\":\"Tue Feb 25 00:49:25 2020 UTC\"}";
        ObjectMapper mapperFrom = new ObjectMapper();
        mapperFrom.setDateFormat(new SimpleDateFormat(JavaAPIMain.DATA_ORIGINAL_DATE_FORMAT, Locale.ENGLISH));

        ObjectMapper mapperTo = new ObjectMapper();
        mapperTo.setDateFormat(new SimpleDateFormat(JavaAPIMain.DATA_ELASTIC_DATE_FORMAT, Locale.ENGLISH));

        Comment c = mapperFrom.readValue(jsonData, Comment.class);
        String transformedJson = mapperTo.writeValueAsString(c);
        System.out.println(transformedJson);
    }
}
