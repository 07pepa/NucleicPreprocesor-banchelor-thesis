package sequence;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.util.*;

/**
 * Created by Jan Kolomazník on 25.6.17.
 * redacted
 */

@Data
@Entity
@Builder
public class Sequence {

    @Id
    @Builder.Default
    @Type(type = "uuid-char")
    private UUID id = UUID.randomUUID();

    @JsonIgnore
    @Type(type = "uuid-char")
    private UUID bufferId;

    private String name;

    @Temporal(TemporalType.TIMESTAMP)
    @CreationTimestamp
    private Date created;

    private SequenceType type;

    private Boolean circular;

    private Integer length;

    private String ncbi;

    @ElementCollection
    private Set<String> tags;

    @ManyToOne(cascade = CascadeType.DETACH)
    @JsonIgnore
    private User owner;

    private String fastaComment;

    @ElementCollection
    private Map<Nucleic, Integer> nucleicCounts;

    public Map<Nucleic, Integer> getNucleicCounts() {
        return (nucleicCounts == null || nucleicCounts.isEmpty())
                ? null
                : Collections.unmodifiableMap(nucleicCounts);
    }
}
