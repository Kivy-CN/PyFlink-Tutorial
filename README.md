# PyFlink-Tutorial

A Brief Tutorial of PyFlink

### By Fatty

### Version 2023.11.15

## Anaconda3 Installation

Download the Anaconda3 package from TUNA first.

```Bash
wget https://mirrors.tuna.tsinghua.edu.cn/anaconda/archive/Anaconda3-2023.09-0-Linux-x86_64.sh
sh Anaconda3-2023.09-0-Linux-x86_64.sh
```
During the installation, please use the default settings.
It should be installed at `~/anaconda3`.

## Python 3.9 Installation

Python 3.9 installed by conda will be easy and reliable.

```Bash
conda create -n pyflink_39 python=3.9
conda activate pyflink_39
```

## Apache-Flink Installation

Then install the apache-flink package with pip.

```Bash
pip install apache-flink
```

## Test with a Flink Python DataStream API Program

The following code comes from the official [documents version 1.18](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/python/datastream_tutorial/).

Save the code below as `DataStream_API_word_count.py`.

```Python
import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy


word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "Or to take arms against a sea of troubles,",
                   "And by opposing end them?--To die,--to sleep,--",
                   "No more; and by a sleep to say we end",
                   "The heartache, and the thousand natural shocks",
                   "That flesh is heir to,--'tis a consummation",
                   "Devoutly to be wish'd. To die,--to sleep;--",
                   "To sleep! perchance to dream:--ay, there's the rub;",
                   "For in that sleep of death what dreams may come,",
                   "When we have shuffled off this mortal coil,",
                   "Must give us pause: there's the respect",
                   "That makes calamity of so long life;",
                   "For who would bear the whips and scorns of time,",
                   "The oppressor's wrong, the proud man's contumely,",
                   "The pangs of despis'd love, the law's delay,",
                   "The insolence of office, and the spurns",
                   "That patient merit of the unworthy takes,",
                   "When he himself might his quietus make",
                   "With a bare bodkin? who would these fardels bear,",
                   "To grunt and sweat under a weary life,",
                   "But that the dread of something after death,--",
                   "The undiscover'd country, from whose bourn",
                   "No traveller returns,--puzzles the will,",
                   "And makes us rather bear those ills we have",
                   "Than fly to others that we know not of?",
                   "Thus conscience does make cowards of us all;",
                   "And thus the native hue of resolution",
                   "Is sicklied o'er with the pale cast of thought;",
                   "And enterprises of great pith and moment,",
                   "With this regard, their currents turn awry,",
                   "And lose the name of action.--Soft you now!",
                   "The fair Ophelia!--Nymph, in thy orisons",
                   "Be all my sins remember'd."]


def word_count(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    if input_path is not None:
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        ds = env.from_collection(word_count_data)

    def split(line):
        yield from line.split()

    # compute word count
    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    word_count(known_args.input, known_args.output)
```

Then run it.

```Bash
python3 DataStream_API_word_count.py
```

You should get the output below.

```Bash
Using Any for unsupported type: typing.Sequence[~T]
No module named google.cloud.bigquery_storage_v1. As a result, the ReadFromBigQuery transform *CANNOT* be used with `method=DIRECT_READ`.
Executing word_count example with default input data set.
Use --input to specify file input.
Printing result to stdout. Use --output to specify output path.
(a,5)
(Be,1)
(Is,1)
(No,2)
(Or,1)
(To,4)
(be,1)
(by,2)
(he,1)
(in,3)
(is,2)
(my,1)
(of,14)
(or,1)
(so,1)
(to,7)
(us,3)
(we,4)
(And,5)
(But,1)
(For,2)
(The,7)
(all,1)
(and,7)
(be,,1)
(end,2)
(fly,1)
(his,1)
(hue,1)
(may,1)
(not,2)
(of?,1)
(off,1)
(say,1)
(sea,1)
(the,15)
(thy,1)
(who,2)
(you,1)
('tis,1)
(Must,1)
(Than,1)
(That,3)
(Thus,1)
(When,2)
(With,2)
(all;,1)
(arms,1)
(bare,1)
(bear,2)
(cast,1)
(does,1)
(fair,1)
(from,1)
(give,1)
(have,2)
(heir,1)
(ills,1)
(know,1)
(long,1)
(lose,1)
(make,2)
(mind,1)
(name,1)
(now!,1)
(o'er,1)
(pale,1)
(pith,1)
(rub;,1)
(sins,1)
(take,1)
(that,3)
(this,2)
(thus,1)
(turn,1)
(what,1)
(with,1)
(after,1)
(awry,,1)
(bear,,1)
(bourn,1)
(coil,,1)
(come,,1)
(death,1)
(dread,1)
(flesh,1)
(great,1)
(grunt,1)
(law's,1)
(life,,1)
(life;,1)
(love,,1)
(makes,2)
(man's,1)
(merit,1)
(might,1)
(more;,1)
(pangs,1)
(proud,1)
(sleep,2)
(sweat,1)
(their,1)
(these,1)
(those,1)
(time,,1)
(under,1)
(weary,1)
(whips,1)
(whose,1)
(will,,1)
(would,2)
(arrows,1)
(delay,,1)
(dreams,1)
(mortal,1)
(native,1)
(nobler,1)
(others,1)
(pause:,1)
(rather,1)
(scorns,1)
(shocks,1)
(sleep!,1)
(slings,1)
(spurns,1)
(suffer,1)
(takes,,1)
(wrong,,1)
(Whether,1)
(against,1)
(bodkin?,1)
(cowards,1)
(fardels,1)
(fortune,1)
(himself,1)
(moment,,1)
(natural,1)
(office,,1)
(orisons,1)
(patient,1)
(quietus,1)
(regard,,1)
(respect,1)
(there's,2)
(wish'd.,1)
(Devoutly,1)
(calamity,1)
(country,,1)
(currents,1)
(death,--,1)
(despis'd,1)
(die,--to,2)
(opposing,1)
(shuffled,1)
(sicklied,1)
(sleep,--,1)
(sleep;--,1)
(thought;,1)
(thousand,1)
(unworthy,1)
(be,--that,1)
(insolence,1)
(perchance,1)
(something,1)
(them?--To,1)
(to,--'tis,1)
(traveller,1)
(troubles,,1)
(conscience,1)
(contumely,,1)
(heartache,,1)
(outrageous,1)
(resolution,1)
(dream:--ay,,1)
(enterprises,1)
(oppressor's,1)
(question:--,1)
(remember'd.,1)
(consummation,1)
(undiscover'd,1)
(action.--Soft,1)
(Ophelia!--Nymph,,1)
(returns,--puzzles,1)
```

That means it works.


## Test with a Flink Python Table API Program

The following code comes from the official [documents version 1.18](https://nightlies.apache.org/flink/flink-docs-release-1.18/zh/docs/dev/python/table_api_tutorial/).

Save the code below as `Table_API_word_count.py`.

```Python
import argparse
import logging
import sys

from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf

word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "Or to take arms against a sea of troubles,",
                   "And by opposing end them?--To die,--to sleep,--",
                   "No more; and by a sleep to say we end",
                   "The heartache, and the thousand natural shocks",
                   "That flesh is heir to,--'tis a consummation",
                   "Devoutly to be wish'd. To die,--to sleep;--",
                   "To sleep! perchance to dream:--ay, there's the rub;",
                   "For in that sleep of death what dreams may come,",
                   "When we have shuffled off this mortal coil,",
                   "Must give us pause: there's the respect",
                   "That makes calamity of so long life;",
                   "For who would bear the whips and scorns of time,",
                   "The oppressor's wrong, the proud man's contumely,",
                   "The pangs of despis'd love, the law's delay,",
                   "The insolence of office, and the spurns",
                   "That patient merit of the unworthy takes,",
                   "When he himself might his quietus make",
                   "With a bare bodkin? who would these fardels bear,",
                   "To grunt and sweat under a weary life,",
                   "But that the dread of something after death,--",
                   "The undiscover'd country, from whose bourn",
                   "No traveller returns,--puzzles the will,",
                   "And makes us rather bear those ills we have",
                   "Than fly to others that we know not of?",
                   "Thus conscience does make cowards of us all;",
                   "And thus the native hue of resolution",
                   "Is sicklied o'er with the pale cast of thought;",
                   "And enterprises of great pith and moment,",
                   "With this regard, their currents turn awry,",
                   "And lose the name of action.--Soft you now!",
                   "The fair Ophelia!--Nymph, in thy orisons",
                   "Be all my sins remember'd."]


def word_count(input_path, output_path):
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # write all the data to one file
    t_env.get_config().set("parallelism.default", "1")

    # define the source
    if input_path is not None:
        t_env.create_temporary_table(
            'source',
            TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .build())
                .option('path', input_path)
                .format('csv')
                .build())
        tab = t_env.from_path('source')
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        tab = t_env.from_elements(map(lambda i: (i,), word_count_data),
                                  DataTypes.ROW([DataTypes.FIELD('line', DataTypes.STRING())]))

    # define the sink
    if output_path is not None:
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .column('count', DataTypes.BIGINT())
                        .build())
                .option('path', output_path)
                .format(FormatDescriptor.for_format('canal-json')
                        .build())
                .build())
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('print')
                .schema(Schema.new_builder()
                        .column('word', DataTypes.STRING())
                        .column('count', DataTypes.BIGINT())
                        .build())
                .build())

    @udtf(result_types=[DataTypes.STRING()])
    def split(line: Row):
        for s in line[0].split():
            yield Row(s)

    # compute word count
    tab.flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), lit(1).count) \
        .execute_insert('sink') \
        .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    word_count(known_args.input, known_args.output)
```

Then run it.

```Bash
python3 Table_API_word_count.py
```

You should get the output below.

```Bash
Using Any for unsupported type: typing.Sequence[~T]
No module named google.cloud.bigquery_storage_v1. As a result, the ReadFromBigQuery transform *CANNOT* be used with `method=DIRECT_READ`.
Executing word_count example with default input data set.
Use --input to specify file input.
Printing result to stdout. Use --output to specify output path.
+I[To, 1]
+I[be,, 1]
+I[or, 1]
+I[not, 1]
+I[to, 1]
+I[be,--that, 1]
+I[is, 1]
+I[the, 1]
+I[question:--, 1]
+I[Whether, 1]
+I['tis, 1]
+I[nobler, 1]
+I[in, 1]
-U[the, 1]
+U[the, 2]
+I[mind, 1]
-U[to, 1]
+U[to, 2]
+I[suffer, 1]
+I[The, 1]
+I[slings, 1]
+I[and, 1]
+I[arrows, 1]
+I[of, 1]
+I[outrageous, 1]
+I[fortune, 1]
+I[Or, 1]
-U[to, 2]
+U[to, 3]
+I[take, 1]
+I[arms, 1]
+I[against, 1]
+I[a, 1]
+I[sea, 1]
-U[of, 1]
+U[of, 2]
+I[troubles,, 1]
+I[And, 1]
+I[by, 1]
+I[opposing, 1]
+I[end, 1]
+I[them?--To, 1]
+I[die,--to, 1]
+I[sleep,--, 1]
+I[No, 1]
+I[more;, 1]
-U[and, 1]
+U[and, 2]
-U[by, 1]
+U[by, 2]
-U[a, 1]
+U[a, 2]
+I[sleep, 1]
-U[to, 3]
+U[to, 4]
+I[say, 1]
+I[we, 1]
-U[end, 1]
+U[end, 2]
-U[The, 1]
+U[The, 2]
+I[heartache,, 1]
-U[and, 2]
+U[and, 3]
-U[the, 2]
+U[the, 3]
+I[thousand, 1]
+I[natural, 1]
+I[shocks, 1]
+I[That, 1]
+I[flesh, 1]
-U[is, 1]
+U[is, 2]
+I[heir, 1]
+I[to,--'tis, 1]
-U[a, 2]
+U[a, 3]
+I[consummation, 1]
+I[Devoutly, 1]
-U[to, 4]
+U[to, 5]
+I[be, 1]
+I[wish'd., 1]
-U[To, 1]
+U[To, 2]
-U[die,--to, 1]
+U[die,--to, 2]
+I[sleep;--, 1]
-U[To, 2]
+U[To, 3]
+I[sleep!, 1]
+I[perchance, 1]
-U[to, 5]
+U[to, 6]
+I[dream:--ay,, 1]
+I[there's, 1]
-U[the, 3]
+U[the, 4]
+I[rub;, 1]
+I[For, 1]
-U[in, 1]
+U[in, 2]
+I[that, 1]
-U[sleep, 1]
+U[sleep, 2]
-U[of, 2]
+U[of, 3]
+I[death, 1]
+I[what, 1]
+I[dreams, 1]
+I[may, 1]
+I[come,, 1]
+I[When, 1]
-U[we, 1]
+U[we, 2]
+I[have, 1]
+I[shuffled, 1]
+I[off, 1]
+I[this, 1]
+I[mortal, 1]
+I[coil,, 1]
+I[Must, 1]
+I[give, 1]
+I[us, 1]
+I[pause:, 1]
-U[there's, 1]
+U[there's, 2]
-U[the, 4]
+U[the, 5]
+I[respect, 1]
-U[That, 1]
+U[That, 2]
+I[makes, 1]
+I[calamity, 1]
-U[of, 3]
+U[of, 4]
+I[so, 1]
+I[long, 1]
+I[life;, 1]
-U[For, 1]
+U[For, 2]
+I[who, 1]
+I[would, 1]
+I[bear, 1]
-U[the, 5]
+U[the, 6]
+I[whips, 1]
-U[and, 3]
+U[and, 4]
+I[scorns, 1]
-U[of, 4]
+U[of, 5]
+I[time,, 1]
-U[The, 2]
+U[The, 3]
+I[oppressor's, 1]
+I[wrong,, 1]
-U[the, 6]
+U[the, 7]
+I[proud, 1]
+I[man's, 1]
+I[contumely,, 1]
-U[The, 3]
+U[The, 4]
+I[pangs, 1]
-U[of, 5]
+U[of, 6]
+I[despis'd, 1]
+I[love,, 1]
-U[the, 7]
+U[the, 8]
+I[law's, 1]
+I[delay,, 1]
-U[The, 4]
+U[The, 5]
+I[insolence, 1]
-U[of, 6]
+U[of, 7]
+I[office,, 1]
-U[and, 4]
+U[and, 5]
-U[the, 8]
+U[the, 9]
+I[spurns, 1]
-U[That, 2]
+U[That, 3]
+I[patient, 1]
+I[merit, 1]
-U[of, 7]
+U[of, 8]
-U[the, 9]
+U[the, 10]
+I[unworthy, 1]
+I[takes,, 1]
-U[When, 1]
+U[When, 2]
+I[he, 1]
+I[himself, 1]
+I[might, 1]
+I[his, 1]
+I[quietus, 1]
+I[make, 1]
+I[With, 1]
-U[a, 3]
+U[a, 4]
+I[bare, 1]
+I[bodkin?, 1]
-U[who, 1]
+U[who, 2]
-U[would, 1]
+U[would, 2]
+I[these, 1]
+I[fardels, 1]
+I[bear,, 1]
-U[To, 3]
+U[To, 4]
+I[grunt, 1]
-U[and, 5]
+U[and, 6]
+I[sweat, 1]
+I[under, 1]
-U[a, 4]
+U[a, 5]
+I[weary, 1]
+I[life,, 1]
+I[But, 1]
-U[that, 1]
+U[that, 2]
-U[the, 10]
+U[the, 11]
+I[dread, 1]
-U[of, 8]
+U[of, 9]
+I[something, 1]
+I[after, 1]
+I[death,--, 1]
-U[The, 5]
+U[The, 6]
+I[undiscover'd, 1]
+I[country,, 1]
+I[from, 1]
+I[whose, 1]
+I[bourn, 1]
-U[No, 1]
+U[No, 2]
+I[traveller, 1]
+I[returns,--puzzles, 1]
-U[the, 11]
+U[the, 12]
+I[will,, 1]
-U[And, 1]
+U[And, 2]
-U[makes, 1]
+U[makes, 2]
-U[us, 1]
+U[us, 2]
+I[rather, 1]
-U[bear, 1]
+U[bear, 2]
+I[those, 1]
+I[ills, 1]
-U[we, 2]
+U[we, 3]
-U[have, 1]
+U[have, 2]
+I[Than, 1]
+I[fly, 1]
-U[to, 6]
+U[to, 7]
+I[others, 1]
-U[that, 2]
+U[that, 3]
-U[we, 3]
+U[we, 4]
+I[know, 1]
-U[not, 1]
+U[not, 2]
+I[of?, 1]
+I[Thus, 1]
+I[conscience, 1]
+I[does, 1]
-U[make, 1]
+U[make, 2]
+I[cowards, 1]
-U[of, 9]
+U[of, 10]
-U[us, 2]
+U[us, 3]
+I[all;, 1]
-U[And, 2]
+U[And, 3]
+I[thus, 1]
-U[the, 12]
+U[the, 13]
+I[native, 1]
+I[hue, 1]
-U[of, 10]
+U[of, 11]
+I[resolution, 1]
+I[Is, 1]
+I[sicklied, 1]
+I[o'er, 1]
+I[with, 1]
-U[the, 13]
+U[the, 14]
+I[pale, 1]
+I[cast, 1]
-U[of, 11]
+U[of, 12]
+I[thought;, 1]
-U[And, 3]
+U[And, 4]
+I[enterprises, 1]
-U[of, 12]
+U[of, 13]
+I[great, 1]
+I[pith, 1]
-U[and, 6]
+U[and, 7]
+I[moment,, 1]
-U[With, 1]
+U[With, 2]
-U[this, 1]
+U[this, 2]
+I[regard,, 1]
+I[their, 1]
+I[currents, 1]
+I[turn, 1]
+I[awry,, 1]
-U[And, 4]
+U[And, 5]
+I[lose, 1]
-U[the, 14]
+U[the, 15]
+I[name, 1]
-U[of, 13]
+U[of, 14]
+I[action.--Soft, 1]
+I[you, 1]
+I[now!, 1]
-U[The, 6]
+U[The, 7]
+I[fair, 1]
+I[Ophelia!--Nymph,, 1]
-U[in, 2]
+U[in, 3]
+I[thy, 1]
+I[orisons, 1]
+I[Be, 1]
+I[all, 1]
+I[my, 1]
+I[sins, 1]
+I[remember'd., 1]
```
