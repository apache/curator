package org.apache.curator.x.rest.entities;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class StatusMessage
{
    private String type;
    private String message;
    private String details;
    private String sourceId;

    public StatusMessage()
    {
        this("", "", "", "");
    }

    public StatusMessage(String type, String sourceId, String message, String details)
    {
        this.type = type;
        this.sourceId = sourceId;
        this.message = message;
        this.details = details;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    public String getDetails()
    {
        return details;
    }

    public void setDetails(String details)
    {
        this.details = details;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public void setSourceId(String sourceId)
    {
        this.sourceId = sourceId;
    }
}
