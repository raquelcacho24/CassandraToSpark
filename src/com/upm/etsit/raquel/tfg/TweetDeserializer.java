package com.upm.etsit.raquel.tfg;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class TweetDeserializer implements Deserializer<Tweet>{

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public Tweet deserialize(String arg0, byte[] arg1) {
		InputStream bis = new ByteArrayInputStream(arg1);
		ObjectInputStream o;
		try {
			o = new ObjectInputStream(bis);
			Tweet test = (Tweet) o.readObject();
			System.out.print(test);
			return test;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}
}
