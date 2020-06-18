import java.io.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.net.URLDecoder;
import java.net.URLEncoder;


public class Prova {


    public static void main(String[] args) {
        Map<String, String> edo = new HashMap<>();
        edo.put("Ciao", "100");
        System.out.println(edo);

        String qwerty = Streamer.mapToString(edo);
        Map<String, String> nuvo = new HashMap<>();
        nuvo = Streamer.stringToMap(qwerty);
        System.out.println(nuvo);


    }
}
