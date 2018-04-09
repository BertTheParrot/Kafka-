package com.openbet.kafta.utils;

import com.google.gson.Gson;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Filter {

    private Set <String> filters = new HashSet <String>();

    public Filter(){

        //initialize fields we are interested in, discard the rest
        filters.add("Customer.id");
        filters.add("Customer.username");
        filters.add("Customer.status");
        filters.add("Customer.langRef");
        filters.add("Customer.PersonalDetails.firstName");
        filters.add("Customer.PersonalDetails.lastName");
        filters.add("Customer.Regisration.source");
        filters.add("Customer.Registration.countryRef");
        filters.add("Customer.CustomerAddress.Address.stateRef");
        filters.add("Customer.CustomerFlags");
        filters.add("Customer.CustomerGroups");
        filters.add("Customer.Account.currencyRef");
    }

    //TODO: Get away from this sucky library

    /**
     * Filter out user info by taking only the information we are interested in,
     * then returning in the form of a JSON string
     * @param input
     * @return filtered String
     * @throws JSONException
     */
    public String filter(String input) throws JSONException {
        // Convert String to Json
        System.out.println(input);
        JSONObject obj = new JSONObject(input);

        //Convert Json object to hashMap + utils
        HashMap<String,String> map = new HashMap<>();
        for (String key : filters  ) {
            map.put(key,obj.getString(key));
        }

        Gson gson = new Gson();
        String json = gson.toJson(map);
        return json;
    }

    public static void main(String... args) throws Exception {
        String start = "{ \"Customer.id\": \"123\", \"Customer.username\": \"123\", \"Customer.status\": \"123\", \"Customer.langRef\": \"123\", \"Customer.PersonalDetails.firstName\": \"123\", \"Customer.PersonalDetails.lastName\": \"123\", \"Customer.Regisration.source\": \"123\", \"Customer.Registration.countryRef\": \"CAN\", \"Customer.CustomerAddress.Address.stateRef\": \"123\", \"Customer.CustomerFlags\": \"123\", \"Customer.CustomerGroups\": \"123\", \"Customer.Account.currencyRef\": \"123\" }";
        System.out.println(start);
        Filter filter = new Filter();
        filter.filter(start);

    }
}
