package com.openbet.kafkaStream.utils;

import com.google.gson.Gson;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Filter {

    private static final String removeKeyFilePath = "./src/main/resources/config/removeKeys";
    private Set<String> remove = new HashSet<>();

    /**
     * Filter out user info by taking only the information we are interested in,
     * then returning in the form of a JSON string
     *
     * @param input
     * @return filtered String
     * @throws JSONException
     */
    public Filter() throws IOException {

        String key = null;
        FileReader fileReader = new FileReader(removeKeyFilePath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        while ((key = bufferedReader.readLine()) != null) {

            if (!key.startsWith("#")) {
                remove.add(key);
            } else {
                System.out.println(key);
            }
        }
    }

    /**
     * Filter out user info by taking only the information we are interested in,
     * then returning in the form of a JSON string
     *
     * @param input
     * @return filtered String
     * @throws JSONException
     */
    public String clean(String input) throws IOException {

        // Convert String to HashMap
        HashMap map = new ObjectMapper().readValue(input, HashMap.class);

        for (String key : remove) {
            map.remove(key);
        }

        Gson gson = new Gson();

        return gson.toJson(map);
    }
}
