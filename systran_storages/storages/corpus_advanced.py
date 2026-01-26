"""Definition of `Corpus Manager Advanced` storage class"""
import logging

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

from systran_storages.storages import Storage

LOGGER = logging.getLogger(__name__)


class CMAdvancedStorage(Storage):
    """Corpus Manager Advanced storage."""

    def __init__(self, storage_id, db_host, db_port, db_name, db_user, db_password):
        super().__init__(storage_id)
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self._connection = None

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    database=self.db_name,
                    user=self.db_user,
                    password=self.db_password,
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
                LOGGER.debug('Connected to database %s', self.db_name)
            except psycopg2.Error as e:
                LOGGER.error('Failed to connect to database: %s', e)
                raise RuntimeError(f'Database connection failed: {e}')

        return self._connection

    def _close_connection(self):
        """Close database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            LOGGER.debug('Closed database connection')

    def __del__(self):
        """Cleanup connection on deletion."""
        self._close_connection()


    def get_random_aligned_segments(self, total, src_language, tgt_language,
                                   src_locale=None, tgt_locale=None,
                                   corpus_name=None, file_pattern=None,
                                   domain=None, formality=None,
                                   src_to_tgt=None, tgt_to_src=None,
                                   min_src_to_tgt_score=None, min_tgt_to_src_score=None):

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            query = """
                SELECT
                    src_s.segment_id as src_segment_id,
                    src_s.text as src_text,
                    src_s.domain as src_domain,
                    src_s.formality as src_formality,
                    src_l.language_code as src_language,
                    src_l.locale_code as src_locale,
                    src_c.corpus_name as src_corpus_name,
                    src_c.file as src_file,
                    tgt_s.segment_id as tgt_segment_id,
                    tgt_s.text as tgt_text,
                    tgt_s.domain as tgt_domain,
                    tgt_s.formality as tgt_formality,
                    tgt_l.language_code as tgt_language,
                    tgt_l.locale_code as tgt_locale,
                    tgt_c.corpus_name as tgt_corpus_name,
                    tgt_c.file as tgt_file,
                    sa.src_to_tgt,
                    sa.tgt_to_src,
                    sa.src_to_tgt_score,
                    sa.tgt_to_src_score,
                    sa.metadata as alignment_metadata
                FROM SegmentAlign sa
                JOIN Segments src_s ON sa.src_segment_id = src_s.segment_id
                JOIN Segments tgt_s ON sa.tgt_segment_id = tgt_s.segment_id
                JOIN Locale src_l ON src_s.language_id = src_l.language_id
                JOIN Locale tgt_l ON tgt_s.language_id = tgt_l.language_id
                JOIN CorpusSegments src_cs ON src_s.segment_id = src_cs.segment_id
                JOIN CorpusSegments tgt_cs ON tgt_s.segment_id = tgt_cs.segment_id
                JOIN Corpus src_c ON src_cs.corpus_id = src_c.corpus_id
                JOIN Corpus tgt_c ON tgt_cs.corpus_id = tgt_c.corpus_id
                WHERE src_cs.is_deleted = FALSE
                  AND tgt_cs.is_deleted = FALSE
                  AND src_l.language_code = %s
                  AND tgt_l.language_code = %s
            """

            params = [src_language, tgt_language]

            if src_locale is not None:
                query += " AND src_l.locale_code = %s"
                params.append(src_locale)

            if tgt_locale is not None:
                query += " AND tgt_l.locale_code = %s"
                params.append(tgt_locale)

            # Apply filters
            if corpus_name:
                query += " AND src_c.corpus_name = %s"
                params.append(corpus_name)

            if file_pattern:
                query += " AND src_c.file LIKE %s"
                params.append(f"%{file_pattern}%")

            if domain:
                query += " AND src_s.domain = %s"
                params.append(domain)

            if formality is not None:
                query += " AND src_s.formality = %s"
                params.append(formality)

            if src_to_tgt is not None:
                query += " AND sa.src_to_tgt = %s"
                params.append(src_to_tgt)

            if tgt_to_src is not None:
                query += " AND sa.tgt_to_src = %s"
                params.append(tgt_to_src)

            if min_src_to_tgt_score is not None:
                query += " AND sa.src_to_tgt_score >= %s"
                params.append(min_src_to_tgt_score)

            if min_tgt_to_src_score is not None:
                query += " AND sa.tgt_to_src_score >= %s"
                params.append(min_tgt_to_src_score)

            query += " ORDER BY RANDOM() LIMIT %s"
            params.append(total)

            LOGGER.info(
                'Fetching %s random aligned pairs (%s-%s -> %s-%s, corpus=%s, file_pattern=%s)',
                total, src_language, src_locale or '*', tgt_language, tgt_locale or '*', corpus_name, file_pattern
            )

            cursor.execute(query, params)
            results = cursor.fetchall()

            pairs = []
            for row in results:
                pair = {
                    'source': {
                        'text': row['src_text'],
                        'language': row['src_language'],
                        'locale': row['src_locale'],
                        'domain': row['src_domain'],
                        'formality': row['src_formality'],
                        'corpus_name': row['src_corpus_name'],
                        'file': row['src_file'],
                        'metadata': {
                            'segment_id': row['src_segment_id']
                        }
                    },
                    'target': {
                        'text': row['tgt_text'],
                        'language': row['tgt_language'],
                        'locale': row['tgt_locale'],
                        'domain': row['tgt_domain'],
                        'formality': row['tgt_formality'],
                        'corpus_name': row['tgt_corpus_name'],
                        'file': row['tgt_file'],
                        'metadata': {
                            'segment_id': row['tgt_segment_id']
                        }
                    },
                    'alignment': {
                        'src_to_tgt': row['src_to_tgt'],
                        'tgt_to_src': row['tgt_to_src'],
                        'src_to_tgt_score': float(row['src_to_tgt_score']) if row['src_to_tgt_score'] is not None else None,
                        'tgt_to_src_score': float(row['tgt_to_src_score']) if row['tgt_to_src_score'] is not None else None
                    }
                }

                if row['alignment_metadata']:
                    pair['alignment']['metadata'] = row['alignment_metadata']

                pairs.append(pair)

            LOGGER.info('Retrieved %s random aligned pairs', len(pairs))
            return pairs

        except psycopg2.Error as e:
            LOGGER.error('Database query failed: %s', e)
            raise RuntimeError(f'Failed to get random aligned segments: {e}')
        finally:
            cursor.close()

    def _internal_path(self, path):
        return path
    def _check_existing_file(self, remote_path, local_path):
        return False
    def _get_file_safe(self, remote_path, local_path):
        raise NotImplementedError()
    def _get_checksum_file(self, local_path):
        return None
    def stream(self, remote_path, buffer_size=1024, stream_format=None):
        raise NotImplementedError()
    def push_file(self, local_path, remote_path, lp=None, is_advanced=False):
        raise NotImplementedError()
    def mkdir(self, remote_path):
        raise NotImplementedError()
    def listdir(self, remote_path, recursive=False, is_file=False, options=None):
        raise NotImplementedError()
    def _delete_single(self, remote_path, isdir):
        raise NotImplementedError()
    def rename(self, old_remote_path, new_remote_path):
        raise NotImplementedError()
    def exists(self, remote_path):
        return False
    def isdir(self, remote_path):
        return False
    def check_for_aliases(self, local_path):
        return None
