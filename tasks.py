@worker_ready.connect
def at_start(signal, sender, **kwargs):
    push_ready_delayed_lead.apply_async([])


@celery.task(name='push_ready_delayed_lead')
def push_ready_delayed_lead(lead_id=None):
    """
        Проход по всем лидам delayed_leads, отправка их в срм, если хватает всех данных.
        Если лид успешно добавлен в срм систему - он удаляется с таблицы delayed_leads и
        добавляется в таблицу published_leads.
    """
    with app.app_context():
        def push_leads(delayed_lead):
            if delayed_lead.source == 'site':
                lead_crm_id, crm_lead = site_lead_process(data=delayed_lead.body, logger=logger)
                if lead_crm_id:
                    published_lead = PublishedLead(
                        crm_id=lead_crm_id,
                        publish_date=get_current_datetime(),
                        school_name=Schools.query.filter_by(crm_id=crm_lead.school_id).first(),
                        country=crm_lead.country,
                        name=f'{crm_lead.first_name} {crm_lead.last_name}',
                        phone=crm_lead.phone,
                        email=crm_lead.email
                    )
                    db.session.add(published_lead)
                    db.session.commit()
                    return True

            elif delayed_lead.source == 'facebook':
                """
                    Используем ту же функцию, что и при обработке входящего запроса с фейсбука. Но, с помощью параметра
                    mode='update_delayed_lead' - отключаем уведомление и дублирование отложенных лидов delayed lead.
                
                """
                lead_body = json.loads(delayed_lead.body)
                if lead_body.get('requestContext') and 'amazonaws.com' in lead_body.get('requestContext').get('domainName'):
                    request_body = json.loads(lead_body.get('body'))
                else:
                    request_body = json.loads(delayed_lead.body)
                logger.debug('Пробегаюсь по отложенным лидам')
                status = facebook_lead_process(request_body=request_body, logger=logger,
                                               mode='update_delayed_lead')
                if status:
                    return True
            return False

        if lead_id:
            delayed_lead = DelayedLead.query.filter_by(id=lead_id).first()
            if delayed_lead:
                logger.debug(f'Начал работать с отложенным лидом id={delayed_lead}')
                result = push_leads(delayed_lead)
                subscribers = TechDepBotSubscribers.query.all()
                if result:
                    db.session.delete(delayed_lead)
                    db.session.commit()
                    message = f'Воркер закончил обработку отложенного лида ID={delayed_lead}'
                    for sub in subscribers:
                        tech_dep_telegram.send_message(sub.chat_id, message)
                    logger.info(f'Worker finish update delayed leads. Updated lead ID={delayed_lead}')
                else:
                    message = f'Воркеру не удалось обработать отложенного лида ID={delayed_lead}'
                    for sub in subscribers:
                        tech_dep_telegram.send_message(sub.chat_id, message)
        else:
            reverse_db_delayed_leads_count = 0
            try:
                conn = pymysql.connect(host=RESERV_DB_HOST, user=RESERV_DB_USER, password=RESERV_DB_PASSWORD,
                                       db=RESERV_DB_NAME, connect_timeout=5)
                if conn.open:
                    with conn.cursor() as cur:
                        cur.execute("""SELECT * FROM `delayed_leads`""")
                        delayed_leads = cur.fetchall()
                        for item in delayed_leads:
                            try:
                                delayed_lead = DelayedLead(
                                    date=item[1],
                                    source=item[2],
                                    body=item[3]
                                )
                                db.session.add(delayed_lead)
                                db.session.commit()
                                reverse_db_delayed_leads_count += 1
                                cur.execute(f"""DELETE FROM `delayed_leads` WHERE `id` = {item[0]}""")
                            except Exception:
                                continue
                    conn.close()
            except Exception:
                pass
            finally:
                d_leads = DelayedLead.query.all()
                updated_delayed_lead = 0
                if d_leads:
                    for delayed_lead in d_leads:
                        logger.debug(f'Начал работать с отложенным лидом id={delayed_lead}')
                        result = push_leads(delayed_lead)
                        if result:
                            db.session.delete(delayed_lead)
                            updated_delayed_lead += 1
                    db.session.commit()

                subscribers = TechDepBotSubscribers.query.all()
                message = f'Воркер закончил обработку отложенных лидов. ' \
                          f'Из резервной базы забрали лидов: {reverse_db_delayed_leads_count}. ' \
                          f'Всего обработано лидов: {updated_delayed_lead} из {len(d_leads)}'
                for sub in subscribers:
                    tech_dep_telegram.send_message(sub.chat_id, message)
                logger.info(f'Worker finish update delayed leads. Updated {updated_delayed_lead} of {len(d_leads)} leads')


@celery.task(name='update_facebook_form_name')
def update_facebook_form_name(fb_app_id, fb_app_secret, access_token, form_id):
    FacebookAdsApi.init(fb_app_id, fb_app_secret, access_token)
    form = LeadgenForm(form_id)
    fields = ['id', 'name']
    params = {}
    try:
        data = form.api_get(fields=fields, params=params)
        form_name = data.get('name')
        if form_name:
            facebook_form = FacebookForm.query.filter_by(id=form_id).first()
            if facebook_form:
                facebook_form.form_name = form_name
                db.session.commit()
    except Exception as e:
        logger.error(e.args)