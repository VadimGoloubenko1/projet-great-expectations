package imt.ibd.lambda;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class AccessLogEntry {
	
	static SimpleDateFormat formater = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", Locale.US);
	
	protected String ip;
	protected Calendar date;
	protected String method;
	protected String url;
	protected int size;
	protected int status;
	
	
	public AccessLogEntry(String ip, Calendar date, String method, String url, int status, int size) {
		this.ip = ip;
		this.date = date;
		this.status = status;
		this.method = method;
		this.url = url;
		this.status = status;
		this.size = size;
	}

	public String getIp() {
		return this.ip;
	}

	public Calendar getDate() {
		return this.date;
	}

	public String getMethod() {
		return method;
	}

	public String getUrl() {
		return url;
	}
	
	public int getStatus() {
		return this.status;
	}
	
	public int getSize() {
		return this.size;
	}
	
	public boolean isSuccess() {
		return this.status == 200;
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(this.getIp()).append(" ");
		buffer.append(formater.format(this.getDate().getTime())).append(" ");
		buffer.append(this.getMethod()).append(" ").append(this.getUrl()).append(" ");
		buffer.append(this.getStatus()).append(" ").append(this.getSize());
		return new String(buffer);
	}
}