package stream;

import java.util.HashMap;
import java.util.Map;


public class Prova {


    public static void main(String[] args) {
        Map<String, String> edo = new HashMap<>();

        System.out.println(edo);

        String qwerty = Streamer.mapToString(edo);
        Map<String, String> nuvo = new HashMap<>();
        nuvo = Streamer.stringToMap(qwerty);
        System.out.println(nuvo);
        System.out.println("Ciso");


    }
}
